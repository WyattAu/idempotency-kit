// Tests talk to a real Redis in docker; unwrap/expect is the test signal.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![cfg(feature = "redis")]

//! Distributed idempotency-store integration tests against a real Redis
//! server (testcontainers, docker required).
//!
//! Each test is `#[ignore]`-gated so a docker-less local `cargo test`
//! stays green; run them explicitly where docker exists:
//!
//! ```sh
//! cargo test --features redis --test redis_idempotency -- --include-ignored
//! ```
//!
//! Proves the properties the in-process store cannot: atomic `SET NX EX`
//! claims across separate store instances (i.e. separate service
//! workers), response replay after completion, and window expiry
//! re-admitting the key.

use std::time::Duration;

use idempotency_kit::{Claim, IdempotencyKey, IdempotencyStore, RedisStore};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

async fn spawn_redis() -> (testcontainers::ContainerAsync<Redis>, RedisStore) {
    let container = Redis::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let url = format!("redis://{host}:{port}/");
    let conn = redis::Client::open(url)
        .unwrap()
        .get_connection_manager()
        .await
        .unwrap();
    (container, RedisStore::new(conn))
}

fn key(scope: &str, body: &[u8]) -> IdempotencyKey {
    IdempotencyKey::derive(scope, body).unwrap()
}

#[tokio::test]
#[ignore = "requires docker (testcontainers Redis)"]
async fn first_claim_wins_then_replays_then_release_reopens() {
    let (_c, store) = spawn_redis().await;
    let k = key("dist", b"order-42");

    assert!(matches!(
        store.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::First
    ));
    assert!(matches!(
        store.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::InFlight
    ));

    store.complete(&k, b"charge-ok".to_vec()).await.unwrap();
    assert!(
        matches!(store.claim(&k, Duration::from_secs(60)).await.unwrap(), Claim::Replay(bytes) if bytes == b"charge-ok"),
        "the completed response must replay across callers"
    );

    store.release(&k).await.unwrap();
    assert!(matches!(
        store.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::First
    ));
}

#[tokio::test]
#[ignore = "requires docker (testcontainers Redis)"]
async fn separate_workers_share_claim_state() {
    // Two store instances over one Redis — the multi-instance deployment
    // shape. The claim is atomic, so only one worker wins.
    let (_c, worker_a) = spawn_redis().await;
    let worker_b = RedisStore::new(worker_a.connection_manager());
    let k = key("workers", b"shared-order");

    assert!(matches!(
        worker_a.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::First
    ));
    assert!(
        matches!(
            worker_b.claim(&k, Duration::from_secs(60)).await.unwrap(),
            Claim::InFlight
        ),
        "worker B must observe worker A's unexpired claim"
    );

    // Completion is visible to the other worker too.
    worker_a.complete(&k, b"done-by-a".to_vec()).await.unwrap();
    assert!(
        matches!(worker_b.claim(&k, Duration::from_secs(60)).await.unwrap(), Claim::Replay(bytes) if bytes == b"done-by-a")
    );
}

#[tokio::test]
#[ignore = "requires docker (testcontainers Redis)"]
async fn distinct_keys_are_independent() {
    let (_c, store) = spawn_redis().await;

    for i in 0..25 {
        let k = key("bulk", format!("order-{i}").as_bytes());
        assert!(matches!(
            store.claim(&k, Duration::from_secs(60)).await.unwrap(),
            Claim::First
        ));
    }
    for i in 0..25 {
        let k = key("bulk", format!("order-{i}").as_bytes());
        assert!(matches!(
            store.claim(&k, Duration::from_secs(60)).await.unwrap(),
            Claim::InFlight
        ));
    }
}

#[tokio::test]
#[ignore = "requires docker (testcontainers Redis)"]
async fn window_expiry_re_admits_keys() {
    let (_c, store) = spawn_redis().await;
    let k = key("expiry", b"two-second-window");

    store.claim(&k, Duration::from_secs(2)).await.unwrap();
    assert!(matches!(
        store.claim(&k, Duration::from_secs(2)).await.unwrap(),
        Claim::InFlight
    ));

    // Past the window the claim's Redis TTL lapses and the key may be
    // claimed again (at-least-once redelivery outside the window is the
    // application's responsibility — the store bounds the window).
    tokio::time::sleep(Duration::from_millis(2_300)).await;
    assert!(matches!(
        store.claim(&k, Duration::from_secs(2)).await.unwrap(),
        Claim::First
    ));
}

#[tokio::test]
#[ignore = "requires docker (testcontainers Redis)"]
async fn minimum_one_second_ttl_is_enforced() {
    let (_c, store) = spawn_redis().await;
    // A zero TTL must not produce an instant-expire claim (EX 0).
    let k = key("floor", b"zero-ttl");
    store.claim(&k, Duration::ZERO).await.unwrap();
    assert!(matches!(
        store.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::InFlight
    ));
}

#[tokio::test]
#[ignore = "requires docker (testcontainers Redis)"]
async fn realistic_pipeline_execute_then_replay_end_to_end() {
    use idempotency_kit::IdempotencyExecutor;

    let (_c, store) = spawn_redis().await;
    let k = key("pipeline", br#"{"order_id":"A1","amount":4200}"#);

    let charge = || async {
        // Charge the card...
        Ok::<_, std::convert::Infallible>(b"{\"status\":\"charged\"}".to_vec())
    };

    // Delivery 1: fresh claim → execute → record response.
    let first = IdempotencyExecutor::execute(&store, &k, Duration::from_secs(60), charge)
        .await
        .unwrap();
    assert_eq!(first, b"{\"status\":\"charged\"}");

    // Delivery 2 (at-least-once redelivery): replays without executing —
    // the sentinel proves the closure never ran.
    let replay = IdempotencyExecutor::execute(&store, &k, Duration::from_secs(60), || async {
        Ok::<_, std::convert::Infallible>(b"sentinel: closure ran".to_vec())
    })
    .await
    .unwrap();
    assert_eq!(replay, first);
}
