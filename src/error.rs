//! Errors produced by key derivation, stores, and the executor.
//!
//! Documented failure modes are part of the API contract; every variant
//! lists the condition that produces it.

/// A failure of the underlying [`IdempotencyStore`](crate::IdempotencyStore).
///
/// Stores are expected to fail closed: when a store cannot safely admit a
/// new claim it returns an error rather than silently dropping state that
/// another caller may depend on.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// The store is at its configured capacity and every tracked claim is
    /// still inside its TTL window. The store refuses the new claim instead
    /// of evicting a live one — evicting would let a duplicate request
    /// execute again, which is precisely what the kit exists to prevent.
    /// See the `memory` feature's `MemoryStore` for the fail-closed rationale.
    #[error("idempotency store at capacity; failing closed")]
    CapacityExceeded,
    /// The backend (Redis, network, serializer) failed. The `String` is the
    /// backend's own diagnostic; the kit never embeds request data in it.
    #[error("idempotency store backend failure: {0}")]
    Backend(String),
}

/// Top-level kit failure: key derivation, store plumbing, or execution
/// outcome.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IdempotencyError {
    /// The scope failed validation. Scopes must match `[a-z0-9_.-]{1,64}`:
    /// lowercase ASCII alphanumerics, `.`, `_`, `-`, 1..=64 characters.
    /// The rejected scope is included for diagnostics.
    #[error("invalid idempotency scope {scope:?}: must match [a-z0-9_.-]{{1,64}}")]
    InvalidScope {
        /// The rejected scope string.
        scope: String,
    },
    /// The underlying store failed while claiming, completing, or
    /// releasing. The claim may or may not have been recorded; retry with a
    /// fresh key is always safe.
    #[error("{0}")]
    Store(#[from] StoreError),
    /// Another caller holds an unexpired, unfinished claim for this key.
    /// The request was not executed; poll or retry later, or let the load
    /// balancer collapse duplicates.
    #[error("concurrent request already in flight for this idempotency key")]
    ConcurrentRequest,
    /// The request closure returned an error. The claim was released, so a
    /// retry can execute again. The `String` is the closure error's
    /// `Display` output.
    #[error("request execution failed: {0}")]
    ExecutionFailed(String),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn store_error_display_is_informative() {
        assert_eq!(
            StoreError::CapacityExceeded.to_string(),
            "idempotency store at capacity; failing closed"
        );
        assert_eq!(
            StoreError::Backend("connection reset".into()).to_string(),
            "idempotency store backend failure: connection reset"
        );
    }

    #[test]
    fn idempotency_error_display_is_informative() {
        let scope = IdempotencyError::InvalidScope {
            scope: "BAD SCOPE".into(),
        };
        assert_eq!(
            scope.to_string(),
            "invalid idempotency scope \"BAD SCOPE\": must match [a-z0-9_.-]{1,64}"
        );
        assert_eq!(
            IdempotencyError::ConcurrentRequest.to_string(),
            "concurrent request already in flight for this idempotency key"
        );
        assert_eq!(
            IdempotencyError::ExecutionFailed("boom".into()).to_string(),
            "request execution failed: boom"
        );
        assert_eq!(
            IdempotencyError::Store(StoreError::CapacityExceeded).to_string(),
            "idempotency store at capacity; failing closed"
        );
    }

    #[test]
    fn store_error_is_source_of_store_variant() {
        use std::error::Error as _;
        let err = IdempotencyError::from(StoreError::Backend("down".into()));
        assert!(err.source().is_some());
        assert!(IdempotencyError::ConcurrentRequest.source().is_none());
    }
}
