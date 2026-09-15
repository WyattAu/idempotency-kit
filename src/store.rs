//! The pluggable storage contract.

use std::time::Duration;

use async_trait::async_trait;

use crate::error::StoreError;
use crate::key::IdempotencyKey;

/// The outcome of an atomic claim attempt.
///
/// [`Claim::First`] and [`Claim::Replay`] are terminal for the caller:
/// execute (First) or return the stored response (Replay).
/// [`Claim::InFlight`] means another caller owns the key right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// This caller owns the claim for `ttl`: run the request, then
    /// [`IdempotencyStore::complete`] (or [`IdempotencyStore::release`] on
    /// failure).
    First,
    /// A completed response exists for this key within its TTL window.
    /// Return the bytes to the caller without executing.
    Replay(Vec<u8>),
    /// Another caller holds an unexpired, unfinished claim. No response
    /// exists yet.
    InFlight,
}

/// Atomic, TTL-bounded idempotency-claim storage.
///
/// Implementations must make [`claim`](IdempotencyStore::claim) atomic
/// across processes: exactly one concurrent caller may observe
/// [`Claim::First`] for a given key at a time. The trait is object-safe
/// (`Arc<dyn IdempotencyStore>` works) so applications can swap stores per
/// deployment tier without generic plumbing.
///
/// Contract notes:
///
/// - TTLs are upper bounds on claim residency; implementations may enforce
///   a floor (the Redis store clamps to 1 second) so a zero TTL cannot
///   produce instant-expire claims.
/// - `complete` records the response so subsequent claims within the TTL
///   window observe [`Claim::Replay`]. Implementations should refresh the
///   response window from `complete` time.
/// - `release` drops a held claim without a response so a retry can claim
///   again; it is the error-path counterpart of `complete`.
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Atomically claim the key for `ttl`. Returns [`Claim::First`] when
    /// this caller owns the claim, [`Claim::Replay`] when a completed
    /// response exists, and [`Claim::InFlight`] when another caller holds
    /// an unexpired, unfinished claim.
    async fn claim(&self, key: &IdempotencyKey, ttl: Duration) -> Result<Claim, StoreError>;

    /// Record the completed response for a held claim.
    async fn complete(&self, key: &IdempotencyKey, response: Vec<u8>) -> Result<(), StoreError>;

    /// Release a claim without a response (error path / rollback).
    async fn release(&self, key: &IdempotencyKey) -> Result<(), StoreError>;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn claim_variants_are_constructible_and_comparable() {
        assert_eq!(Claim::First, Claim::First);
        assert_eq!(
            Claim::Replay(b"resp".to_vec()),
            Claim::Replay(b"resp".to_vec())
        );
        assert_ne!(Claim::First, Claim::InFlight);
        let debug = format!("{:?}", Claim::Replay(vec![1]));
        assert!(debug.contains("Replay"));
    }
}
