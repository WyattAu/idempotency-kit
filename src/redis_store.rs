//! Distributed idempotency store backed by Redis.

use std::time::Duration;

use async_trait::async_trait;
use redis::aio::ConnectionManager;

use crate::error::StoreError;
use crate::key::IdempotencyKey;
use crate::store::{Claim, IdempotencyStore};

/// Namespace prefix for every Redis key this store touches.
const NAMESPACE: &str = "idempotency-kit";

/// Process-shared [`IdempotencyStore`] backed by Redis.
///
/// Claims are `SET key val NX EX ttl` — an atomic compare-and-set, so
/// multi-instance deployments agree on a single owner per key without
/// Lua. The completed response is stored under `{key}:resp` with the
/// claim key's remaining TTL, so claim and response windows lapse
/// together.
///
/// The store uses a [`ConnectionManager`] (multiplexed, auto-reconnecting)
/// like the rest of the estate: clone the manager per deployment tier and
/// share one connection per process.
#[derive(Clone)]
pub struct RedisStore {
    conn: ConnectionManager,
}

impl RedisStore {
    /// Create a store sharing the given connection manager.
    #[must_use]
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }

    /// A clone of the underlying connection manager, so one multiplexed
    /// connection per process can back several stores or caches.
    #[must_use]
    pub fn connection_manager(&self) -> ConnectionManager {
        self.conn.clone()
    }

    /// The Redis key tracking the claim: `"{namespace}:{scope}:{hex}"`.
    fn claim_key(key: &IdempotencyKey) -> String {
        format!("{NAMESPACE}:{}", key.as_str())
    }

    /// The Redis key holding the completed response. Suffix-disjoint from
    /// [`claim_key`](Self::claim_key): the hex tail is always 64
    /// characters, so no claim key can collide with a `:resp` suffix.
    fn resp_key(key: &IdempotencyKey) -> String {
        format!("{NAMESPACE}:{}:resp", key.as_str())
    }
}

/// TTL in whole seconds for a claim, floored at 1 so a zero TTL cannot
/// produce an instant-expire claim (`EX 0` would be rejected by Redis).
fn claim_ttl_secs(ttl: Duration) -> i64 {
    let secs = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
    secs.max(1)
}

/// TTL in whole seconds for a stored response: the claim key's remaining
/// TTL. Redis reports `-2` (missing) or `-1` (no expiry) for degenerate
/// cases; both floor to a 1-second window so a completed response is
/// still observable briefly.
fn response_ttl_secs(remaining: i64) -> i64 {
    remaining.max(1)
}

/// Pure claim decision, shared by the Redis round-trips: a successful
/// `SET NX` wins outright; otherwise an existing response replays and
/// everything else is an in-flight collision.
fn claim_outcome(claimed: bool, response: Option<Vec<u8>>) -> Claim {
    if claimed {
        Claim::First
    } else if let Some(response) = response {
        Claim::Replay(response)
    } else {
        Claim::InFlight
    }
}

/// The backend diagnostic mapping. Owns the error because `map_err` on
/// the redis futures requires an owned-input closure; the string is
/// extracted before the error is dropped.
#[allow(clippy::needless_pass_by_value)]
fn backend(err: redis::RedisError) -> StoreError {
    StoreError::Backend(err.to_string())
}

#[async_trait]
impl IdempotencyStore for RedisStore {
    async fn claim(&self, key: &IdempotencyKey, ttl: Duration) -> Result<Claim, StoreError> {
        let claim_key = Self::claim_key(key);
        let resp_key = Self::resp_key(key);
        let mut conn = self.conn.clone();

        let claimed: Option<String> = redis::cmd("SET")
            .arg(&claim_key)
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(claim_ttl_secs(ttl))
            .query_async(&mut conn)
            .await
            .map_err(backend)?;
        if claimed.is_some() {
            return Ok(Claim::First);
        }

        // Claim held elsewhere: a completed response replays, everything
        // else is an unfinished collision.
        let response: Option<Vec<u8>> = redis::cmd("GET")
            .arg(&resp_key)
            .query_async(&mut conn)
            .await
            .map_err(backend)?;
        Ok(claim_outcome(false, response))
    }

    async fn complete(&self, key: &IdempotencyKey, response: Vec<u8>) -> Result<(), StoreError> {
        let claim_key = Self::claim_key(key);
        let resp_key = Self::resp_key(key);
        let mut conn = self.conn.clone();

        // Mirror the claim's remaining window onto the response: both
        // lapse together, exactly like the in-memory store.
        let remaining: i64 = redis::cmd("TTL")
            .arg(&claim_key)
            .query_async(&mut conn)
            .await
            .map_err(backend)?;
        redis::cmd("SETEX")
            .arg(&resp_key)
            .arg(response_ttl_secs(remaining))
            .arg(response)
            .query_async::<()>(&mut conn)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn release(&self, key: &IdempotencyKey) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        redis::cmd("DEL")
            .arg(Self::claim_key(key))
            .arg(Self::resp_key(key))
            .query_async::<i64>(&mut conn)
            .await
            .map_err(backend)?;
        Ok(())
    }
}

#[cfg(all(test, feature = "redis"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn keys_are_namespaced_and_suffix_disjoint() {
        let key = IdempotencyKey::derive("orders", b"payload").unwrap();
        let claim = RedisStore::claim_key(&key);
        let resp = RedisStore::resp_key(&key);
        assert_eq!(claim, format!("idempotency-kit:{key}"));
        assert_eq!(resp, format!("{claim}:resp"));
        // The claim-key charset (scope excludes ':', hex is 64 chars)
        // guarantees no claim key ever ends in ":resp".
        assert!(!claim.ends_with(":resp"));
    }

    #[test]
    fn claim_ttl_is_floored_at_one_second() {
        assert_eq!(claim_ttl_secs(Duration::ZERO), 1);
        assert_eq!(claim_ttl_secs(Duration::from_millis(1_500)), 1);
        assert_eq!(claim_ttl_secs(Duration::from_secs(300)), 300);
        // u64::MAX secs must not wrap the i64 cast.
        assert_eq!(claim_ttl_secs(Duration::from_secs(u64::MAX)), i64::MAX);
    }

    #[test]
    fn response_ttl_floors_degenerate_remaining_values() {
        assert_eq!(response_ttl_secs(-2), 1, "missing claim key");
        assert_eq!(response_ttl_secs(-1), 1, "claim key without expiry");
        assert_eq!(response_ttl_secs(0), 1);
        assert_eq!(response_ttl_secs(42), 42);
    }

    #[test]
    fn claim_outcome_precedence() {
        assert_eq!(claim_outcome(true, None), Claim::First);
        // A successful SET NX wins even if a stale response lingers.
        assert_eq!(claim_outcome(true, Some(b"stale".to_vec())), Claim::First);
        assert_eq!(
            claim_outcome(false, Some(b"resp".to_vec())),
            Claim::Replay(b"resp".to_vec())
        );
        assert_eq!(claim_outcome(false, None), Claim::InFlight);
    }
}
