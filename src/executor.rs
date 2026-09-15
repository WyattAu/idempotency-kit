//! The claim → execute → complete/release driver.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use crate::error::IdempotencyError;
use crate::key::IdempotencyKey;
use crate::store::{Claim, IdempotencyStore};

/// Executes a request closure under an idempotency claim.
///
/// The executor is the kit's contract in code:
///
/// 1. **Claim.** [`Claim::First`] → this caller runs the request.
///    [`Claim::Replay`] → a completed response exists; return it without
///    executing. [`Claim::InFlight`] → another caller is mid-request;
///    fail with [`IdempotencyError::ConcurrentRequest`].
/// 2. **Complete or release.** A successful request is recorded with
///    [`IdempotencyStore::complete`] so subsequent claims replay it; a
///    failed request releases the claim so a retry can execute again.
///
/// # Exactly-once response, at-least-once execution
///
/// The kit provides exactly-once *response replay*, not exactly-once
/// execution. Side effects inside the closure may run more than once
/// across a TTL boundary (a claim lapses while its request is still
/// running; the retry then executes again). Callers with non-idempotent
/// side effects must make those effects idempotent themselves — the kit
/// guarantees only that the same response bytes are returned within the
/// window.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdempotencyExecutor;

impl IdempotencyExecutor {
    /// Claim `key` for `ttl`, run `f` at most once per claim, and replay
    /// the recorded response for every subsequent claim inside the window.
    ///
    /// `f` receives no arguments; capture what the request needs in its
    /// closure. Its error is stringified into
    /// [`IdempotencyError::ExecutionFailed`] after the claim is released.
    ///
    /// # Errors
    ///
    /// - [`IdempotencyError::Store`]: the store failed on any step. A
    ///   failed `complete` means the response was computed but not
    ///   persisted; the next caller may re-execute (at-least-once).
    /// - [`IdempotencyError::ConcurrentRequest`]: another caller holds the
    ///   claim; the closure was not run.
    /// - [`IdempotencyError::ExecutionFailed`]: the closure failed; the
    ///   claim was released. If the release itself failed, the claim
    ///   lapses via its TTL instead — retrying waits for the window.
    pub async fn execute<S, F, Fut, E>(
        store: &S,
        key: &IdempotencyKey,
        ttl: Duration,
        f: F,
    ) -> Result<Vec<u8>, IdempotencyError>
    where
        S: IdempotencyStore + ?Sized,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>, E>> + Send,
        E: fmt::Display,
    {
        match store.claim(key, ttl).await? {
            Claim::First => match f().await {
                Ok(response) => {
                    store.complete(key, response.clone()).await?;
                    Ok(response)
                }
                Err(err) => {
                    let message = err.to_string();
                    // Best-effort rollback: if the release fails, the
                    // claim lapses via its TTL and retries observe
                    // InFlight until it does.
                    let _ = store.release(key).await;
                    Err(IdempotencyError::ExecutionFailed(message))
                }
            },
            Claim::Replay(response) => Ok(response),
            Claim::InFlight => Err(IdempotencyError::ConcurrentRequest),
        }
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::error::StoreError;
    use crate::memory::MemoryStore;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn key(body: &[u8]) -> IdempotencyKey {
        IdempotencyKey::derive("exec", body).unwrap()
    }

    #[tokio::test]
    async fn happy_path_runs_once_then_replays() {
        let store = MemoryStore::new();
        let k = key(b"once");
        let runs = Arc::new(AtomicUsize::new(0));

        let run = |runs: Arc<AtomicUsize>| {
            move || {
                let runs = runs.clone();
                async move {
                    runs.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, std::convert::Infallible>(b"response".to_vec())
                }
            }
        };

        let first =
            IdempotencyExecutor::execute(&store, &k, Duration::from_secs(60), run(runs.clone()))
                .await
                .unwrap();
        assert_eq!(first, b"response");

        let replay =
            IdempotencyExecutor::execute(&store, &k, Duration::from_secs(60), run(runs.clone()))
                .await
                .unwrap();
        assert_eq!(replay, b"response");
        assert_eq!(runs.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn closure_error_releases_the_claim() {
        let store = MemoryStore::new();
        let k = key(b"fails-first");

        let err = IdempotencyExecutor::execute(&store, &k, Duration::from_secs(60), || async {
            Err::<Vec<u8>, _>("disk on fire")
        })
        .await
        .unwrap_err();
        assert_eq!(
            err,
            IdempotencyError::ExecutionFailed("disk on fire".into())
        );
        assert!(store.is_empty(), "failed execution must release the claim");

        // The retry executes again and succeeds.
        let retry = IdempotencyExecutor::execute(&store, &k, Duration::from_secs(60), || async {
            Ok::<_, std::convert::Infallible>(b"recovered".to_vec())
        })
        .await
        .unwrap();
        assert_eq!(retry, b"recovered");
    }

    #[tokio::test]
    async fn concurrent_caller_gets_in_flight() {
        let store = MemoryStore::new();
        let k = key(b"contended");

        // Hold the claim without completing: the second caller must see
        // ConcurrentRequest, not execute.
        store.claim(&k, Duration::from_secs(60)).await.unwrap();
        let err = IdempotencyExecutor::execute(&store, &k, Duration::from_secs(60), || async {
            Ok::<Vec<u8>, std::convert::Infallible>(b"must not run".to_vec())
        })
        .await
        .unwrap_err();
        assert_eq!(err, IdempotencyError::ConcurrentRequest);
    }

    #[tokio::test]
    async fn store_errors_surface_as_store_variant() {
        // A full store cannot even record the claim.
        let store = MemoryStore::with_capacity(1);
        let blocker = key(b"blocker");
        store
            .claim(&blocker, Duration::from_secs(60))
            .await
            .unwrap();

        let err = IdempotencyExecutor::execute(
            &store,
            &key(b"over"),
            Duration::from_secs(60),
            || async { Ok::<Vec<u8>, std::convert::Infallible>(vec![]) },
        )
        .await
        .unwrap_err();
        assert_eq!(err, IdempotencyError::Store(StoreError::CapacityExceeded));
    }
}
