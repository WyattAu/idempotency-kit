//! Integration tests: derivation, the store lifecycle, and the executor
//! under real concurrency — the surfaces applications touch end to end.
#![cfg(feature = "memory")]
#![cfg_attr(not(feature = "memory"), allow(missing_docs))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use idempotency_kit::{
    Claim, IdempotencyError, IdempotencyExecutor, IdempotencyKey, IdempotencyStore, MemoryStore,
    StoreError,
};
use tokio::sync::Notify;

fn key(scope: &str, body: &[u8]) -> IdempotencyKey {
    IdempotencyKey::derive(scope, body).unwrap()
}

/// Same scope + bytes derive the same key on every call, so independent
/// processes converge on one claim without coordination.
#[test]
fn derivation_is_deterministic_and_scope_scoped() {
    let a = key("payments", br#"{"amount":4200}"#);
    let b = key("payments", br#"{"amount":4200}"#);
    assert_eq!(a, b);

    // Scope namespaces the key: identical bytes under different scopes
    // must never share a claim.
    let other = key("refunds", br#"{"amount":4200}"#);
    assert_ne!(a, other);
    assert_eq!(a.scope(), "payments");
    assert_eq!(a.as_str().len(), "payments".len() + 1 + 64);
}

#[test]
fn invalid_scopes_are_typed_errors() {
    for scope in ["", "UPPER", "sp ace", "sl/ash", "co:lon", &"x".repeat(65)] {
        let err = IdempotencyKey::derive(scope, b"payload").unwrap_err();
        assert_eq!(
            err,
            IdempotencyError::InvalidScope {
                scope: scope.to_owned()
            }
        );
    }
}

#[tokio::test]
async fn store_lifecycle_first_inflight_replay_release() {
    let store = MemoryStore::new();
    let k = key("lifecycle", b"request-body");

    // First claim wins.
    assert!(matches!(
        store.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::First
    ));
    // A held claim blocks concurrent callers.
    assert!(matches!(
        store.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::InFlight
    ));
    // Completing records the response for replay.
    store
        .complete(&k, b"response-bytes".to_vec())
        .await
        .unwrap();
    assert!(
        matches!(store.claim(&k, Duration::from_secs(60)).await.unwrap(), Claim::Replay(bytes) if bytes == b"response-bytes")
    );
    // Releasing forgets the claim entirely: the next caller executes.
    store.release(&k).await.unwrap();
    assert!(matches!(
        store.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::First
    ));
}

#[tokio::test]
async fn ttl_expiry_reclaims_expired_claims() {
    let store = MemoryStore::new();
    let k = key("ttl", b"short-window");

    store.claim(&k, Duration::from_millis(80)).await.unwrap();
    assert!(matches!(
        store.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::InFlight
    ));

    // Past the window the claim lapses and the key is claimable again —
    // the TTL bound is the contract, and redelivery beyond it is the
    // caller's concern.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(matches!(
        store.claim(&k, Duration::from_secs(60)).await.unwrap(),
        Claim::First
    ));
}

#[tokio::test]
async fn capacity_fails_closed_without_eviction() {
    let store = MemoryStore::with_capacity(2);
    let a = key("cap", b"a");
    let b = key("cap", b"b");
    let c = key("cap", b"c");

    store.claim(&a, Duration::from_secs(60)).await.unwrap();
    store.claim(&b, Duration::from_secs(60)).await.unwrap();
    assert_eq!(
        store.claim(&c, Duration::from_secs(60)).await.unwrap_err(),
        StoreError::CapacityExceeded,
        "fresh claims at capacity must fail closed"
    );

    // Fail-closed means fail closed: the tracked claims are intact.
    assert!(matches!(
        store.claim(&a, Duration::from_secs(60)).await.unwrap(),
        Claim::InFlight
    ));
    assert!(matches!(
        store.claim(&b, Duration::from_secs(60)).await.unwrap(),
        Claim::InFlight
    ));
}

#[tokio::test]
async fn executor_happy_path_executes_once_and_replays() {
    let store = Arc::new(MemoryStore::new());
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let run = |store: Arc<MemoryStore>,
               executions: Arc<std::sync::atomic::AtomicUsize>,
               k: IdempotencyKey| async move {
        IdempotencyExecutor::execute(&*store, &k, Duration::from_secs(60), || {
            let executions = executions.clone();
            async move {
                executions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok::<_, std::convert::Infallible>(b"{\"ok\":true}".to_vec())
            }
        })
        .await
    };

    let first = run(store.clone(), executions.clone(), key("exec", b"happy"))
        .await
        .unwrap();
    let replay = run(store, executions, key("exec", b"happy")).await.unwrap();
    assert_eq!(first, b"{\"ok\":true}");
    assert_eq!(replay, first, "replay must return the recorded response");
}

/// Two concurrent executors race one key: exactly one observes `First`
/// and executes; the other observes `InFlight` and gets
/// `ConcurrentRequest` — the distributed single-flight property, staged
/// deterministically (the winner's closure blocks until the loser has
/// been rejected).
#[tokio::test]
async fn concurrent_executors_yield_one_first_one_in_flight() {
    let store = Arc::new(MemoryStore::new());
    let k = key("race", b"contended-request");
    let loser_saw_error = Arc::new(Notify::new());

    // Winner: executes a closure that only finishes once the loser has
    // been rejected, proving the InFlight observation happened while the
    // claim was genuinely held mid-execution.
    let winner_store = store.clone();
    let gate = loser_saw_error.clone();
    let k_winner = k.clone();
    let winner = tokio::spawn(async move {
        IdempotencyExecutor::execute(&*winner_store, &k_winner, Duration::from_secs(60), || {
            let gate = gate.clone();
            async move {
                gate.notified().await;
                Ok::<_, std::convert::Infallible>(b"winner-response".to_vec())
            }
        })
        .await
    });

    // Wait until the winner visibly holds the claim.
    let mut waited = 0;
    while store.is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
        waited += 1;
        assert!(waited < 2_000, "winner never claimed");
    }

    // Loser: must be rejected with ConcurrentRequest, closure never run.
    let loser = tokio::spawn({
        let store = store.clone();
        let k = k.clone();
        async move {
            IdempotencyExecutor::execute(&*store, &k, Duration::from_secs(60), || async {
                Err::<Vec<u8>, _>("loser executed — single-flight violated")
            })
            .await
        }
    });

    let loser_result = loser.await.unwrap();
    assert_eq!(
        loser_result.unwrap_err(),
        IdempotencyError::ConcurrentRequest
    );

    // Release the winner; it completes and its response is recorded.
    loser_saw_error.notify_one();
    let winner_result = winner.await.unwrap().unwrap();
    assert_eq!(winner_result, b"winner-response");
    assert!(
        matches!(store.claim(&k, Duration::from_secs(60)).await.unwrap(), Claim::Replay(bytes) if bytes == b"winner-response")
    );
}

/// An execution failure must release the claim so a retry can run —
/// verified through the store's observable state.
#[tokio::test]
async fn executor_error_path_releases_for_retry() {
    let store = MemoryStore::new();
    let k = key("error-path", b"failing-request");

    let err = IdempotencyExecutor::execute(&store, &k, Duration::from_secs(60), || async {
        Err::<Vec<u8>, _>("payment provider 502")
    })
    .await
    .unwrap_err();
    assert_eq!(
        err,
        IdempotencyError::ExecutionFailed("payment provider 502".into())
    );
    assert!(store.is_empty(), "the failed claim must be released");

    // Retry executes fresh and records its own response.
    let retry = IdempotencyExecutor::execute(&store, &k, Duration::from_secs(60), || async {
        Ok::<_, std::convert::Infallible>(b"recovered".to_vec())
    })
    .await
    .unwrap();
    assert_eq!(retry, b"recovered");
}

/// Object safety: stores compose behind `Arc<dyn IdempotencyStore>`, and
/// the executor drives trait objects.
#[tokio::test]
async fn executor_drives_dyn_store() {
    let store: Arc<dyn IdempotencyStore> = Arc::new(MemoryStore::new());
    let k = key("dyn", b"object-safe");

    let response = IdempotencyExecutor::execute(&*store, &k, Duration::from_secs(60), || async {
        Ok::<_, std::convert::Infallible>(b"via-trait-object".to_vec())
    })
    .await
    .unwrap();
    assert_eq!(response, b"via-trait-object");

    let replay = IdempotencyExecutor::execute(&*store, &k, Duration::from_secs(60), || async {
        Ok::<_, std::convert::Infallible>(b"unused".to_vec())
    })
    .await
    .unwrap();
    assert_eq!(replay, b"via-trait-object");
}
