//! In-process idempotency store.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use crate::error::StoreError;
use crate::key::IdempotencyKey;
use crate::store::{Claim, IdempotencyStore};

/// Default capacity: 65,536 concurrently-tracked claims.
pub const DEFAULT_CAPACITY: usize = 65_536;

/// A full expiry sweep runs at most once per this many fresh-claim
/// inserts (a sweep is also forced whenever the store is at capacity).
const SWEEP_EVERY: usize = 1024;

/// One tracked key: an unfinished claim or a recorded response.
#[derive(Debug, Clone)]
enum Slot {
    /// Claimed, no response yet. The claim lapses at `deadline`.
    Claimed { deadline: Instant },
    /// Completed response, replayable until `deadline`. The response
    /// window is the claim window: `complete` records the response with
    /// the deadline the claim already had, mirroring the Redis store where
    /// the response key's remaining TTL matches the claim key's.
    Done {
        deadline: Instant,
        response: Vec<u8>,
    },
}

impl Slot {
    fn deadline(&self) -> Instant {
        match self {
            Self::Claimed { deadline } | Self::Done { deadline, .. } => *deadline,
        }
    }

    fn is_live(&self, now: Instant) -> bool {
        self.deadline() > now
    }
}

/// Process-local [`IdempotencyStore`] backed by a sharded concurrent map.
///
/// # Semantics (fail-closed)
///
/// Capacity is bounded: the store tracks at most
/// [`MemoryStore::capacity`] live claims. Expired entries are pruned
/// lazily — a sweep runs every 1,024 fresh-claim inserts, and is
/// forced whenever the store is at capacity — but if every tracked claim
/// is still inside its TTL window, `claim` returns
/// [`StoreError::CapacityExceeded`] rather than forgetting a claim.
///
/// Forgetting a held claim would admit a duplicate execution of a request
/// that may still be mid-flight somewhere else; rejecting new claims is
/// the safe failure mode. Callers that hit this error should shed load or
/// fail the request — exactly the behavior you want from an idempotency
/// layer under memory pressure.
///
/// Two deliberate asymmetries:
///
/// - `complete` never fails closed. A computed response that cannot be
///   recorded is wasted work and would force the next caller to re-execute
///   side effects, so `complete` upserts even past the soft capacity bound
///   (bounded above by the number of in-flight requests).
/// - A claim whose deadline passed is reclaimable (`Claim::First` again):
///   the TTL window is the contract, and the deadline is enforced lazily
///   on access.
///
/// For multi-instance deployments use the `redis` feature's
/// `RedisStore`, which shares claim state atomically
/// across processes.
#[derive(Debug)]
pub struct MemoryStore {
    map: DashMap<String, Slot>,
    capacity: usize,
    fresh_inserts_since_sweep: AtomicUsize,
}

impl MemoryStore {
    /// Create a store with the default capacity of [`DEFAULT_CAPACITY`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a store with an explicit capacity bound (minimum 1).
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: DashMap::new(),
            capacity: capacity.max(1),
            fresh_inserts_since_sweep: AtomicUsize::new(0),
        }
    }

    /// The configured capacity bound.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of tracked slots (may include not-yet-pruned expired
    /// entries).
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether no slots are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop every expired slot immediately (tests and administrative
    /// callers; the production path prunes lazily).
    pub fn sweep_expired(&self) {
        let now = Instant::now();
        self.map.retain(|_, slot| slot.is_live(now));
    }

    /// Sweep on the fresh-claim path: every 1,024 inserts, and
    /// always when the store is at capacity (mirrors webhookkit's
    /// replay-guard cadence).
    fn maybe_sweep(&self, now: Instant) {
        let inserts = self
            .fresh_inserts_since_sweep
            .fetch_add(1, Ordering::Relaxed);
        if inserts >= SWEEP_EVERY || self.map.len() >= self.capacity {
            self.fresh_inserts_since_sweep.store(0, Ordering::Relaxed);
            self.map.retain(|_, slot| slot.is_live(now));
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IdempotencyStore for MemoryStore {
    async fn claim(&self, key: &IdempotencyKey, ttl: Duration) -> Result<Claim, StoreError> {
        let now = Instant::now();
        let deadline = now + ttl;

        // Fast path: the key is tracked. Live slots decide the outcome
        // without touching the sweep counter.
        if let Some(mut slot) = self.map.get_mut(key.as_str()) {
            if slot.is_live(now) {
                return Ok(match &*slot {
                    Slot::Claimed { .. } => Claim::InFlight,
                    Slot::Done { response, .. } => Claim::Replay(response.clone()),
                });
            }
            // Expired: reclaim in place under the entry lock (atomic).
            *slot = Slot::Claimed { deadline };
            return Ok(Claim::First);
        }

        // Fresh path: sweep when due, then bound capacity before
        // admitting a new claim. The len check races with concurrent
        // inserts, so the winner is decided after insertion below.
        self.maybe_sweep(now);
        match self.map.entry(key.as_str().to_owned()) {
            Entry::Occupied(mut occupied) => {
                // Lost a same-key race: another caller inserted between
                // our get_mut and entry.
                if occupied.get().is_live(now) {
                    return Ok(match occupied.get() {
                        Slot::Claimed { .. } => Claim::InFlight,
                        Slot::Done { response, .. } => Claim::Replay(response.clone()),
                    });
                }
                occupied.insert(Slot::Claimed { deadline });
                Ok(Claim::First)
            }
            Entry::Vacant(vacant) => {
                vacant.insert(Slot::Claimed { deadline });
                if self.map.len() > self.capacity {
                    // Lost a distinct-key race past the bound: roll back
                    // our own insert and fail closed.
                    self.map.remove(key.as_str());
                    return Err(StoreError::CapacityExceeded);
                }
                Ok(Claim::First)
            }
        }
    }

    async fn complete(&self, key: &IdempotencyKey, response: Vec<u8>) -> Result<(), StoreError> {
        let now = Instant::now();
        match self.map.entry(key.as_str().to_owned()) {
            Entry::Occupied(mut occupied) => {
                // Keep the claim's window: the response is replayable for
                // exactly as long as the claim would have lived.
                let deadline = occupied.get().deadline().max(now);
                occupied.insert(Slot::Done { deadline, response });
            }
            // The claim was released or swept before completion; record
            // the response anyway with a lapsed window so the slot is
            // reclaimed on next claim rather than failing closed here.
            Entry::Vacant(vacant) => {
                vacant.insert(Slot::Done {
                    deadline: now,
                    response,
                });
            }
        }
        Ok(())
    }

    async fn release(&self, key: &IdempotencyKey) -> Result<(), StoreError> {
        self.map.remove(key.as_str());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn key(scope: &str, body: &[u8]) -> IdempotencyKey {
        IdempotencyKey::derive(scope, body).unwrap()
    }

    #[tokio::test]
    async fn claim_complete_replay_release_lifecycle() {
        let store = MemoryStore::new();
        let k = key("orders", b"payload");

        assert!(matches!(
            store.claim(&k, Duration::from_secs(60)).await,
            Ok(Claim::First)
        ));
        // Unfinished claim is held: another caller sees InFlight.
        assert!(matches!(
            store.claim(&k, Duration::from_secs(60)).await,
            Ok(Claim::InFlight)
        ));

        store.complete(&k, b"resp-1".to_vec()).await.unwrap();
        assert!(
            matches!(store.claim(&k, Duration::from_secs(60)).await, Ok(Claim::Replay(bytes)) if bytes == b"resp-1"),
            "completed claim must replay the recorded response"
        );

        store.release(&k).await.unwrap();
        assert!(matches!(
            store.claim(&k, Duration::from_secs(60)).await,
            Ok(Claim::First)
        ));
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn ttl_expiry_reclaims_the_claim() {
        let store = MemoryStore::new();
        let k = key("orders", b"expiring");

        assert!(matches!(
            store.claim(&k, Duration::from_millis(80)).await,
            Ok(Claim::First)
        ));
        assert!(matches!(
            store.claim(&k, Duration::from_secs(60)).await,
            Ok(Claim::InFlight)
        ));

        tokio::time::sleep(Duration::from_millis(140)).await;
        // Lazy expiry: still tracked, but claimable again.
        assert_eq!(store.len(), 1);
        assert!(matches!(
            store.claim(&k, Duration::from_secs(60)).await,
            Ok(Claim::First)
        ));
    }

    #[tokio::test]
    async fn expired_response_window_is_reclaimable() {
        let store = MemoryStore::new();
        let k = key("orders", b"short-replay");

        store.claim(&k, Duration::from_millis(80)).await.unwrap();
        store.complete(&k, b"stale".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(140)).await;
        assert!(
            matches!(
                store.claim(&k, Duration::from_secs(60)).await,
                Ok(Claim::First)
            ),
            "a lapsed response must not replay"
        );
    }

    #[tokio::test]
    async fn capacity_fails_closed_with_live_claims() {
        let store = MemoryStore::with_capacity(2);
        let a = key("cap", b"a");
        let b = key("cap", b"b");
        let c = key("cap", b"c");

        store.claim(&a, Duration::from_secs(60)).await.unwrap();
        store.claim(&b, Duration::from_secs(60)).await.unwrap();
        assert!(
            matches!(
                store.claim(&c, Duration::from_secs(60)).await,
                Err(StoreError::CapacityExceeded)
            ),
            "at capacity with fresh claims the store must fail closed"
        );

        // The tracked claims are untouched — no silent eviction.
        assert!(matches!(
            store.claim(&a, Duration::from_secs(60)).await,
            Ok(Claim::InFlight)
        ));
        assert!(matches!(
            store.claim(&b, Duration::from_secs(60)).await,
            Ok(Claim::InFlight)
        ));

        // Release frees a slot for the third key.
        store.release(&a).await.unwrap();
        assert!(matches!(
            store.claim(&c, Duration::from_secs(60)).await,
            Ok(Claim::First)
        ));
    }

    #[tokio::test]
    async fn expired_slots_are_swept_when_at_capacity() {
        let store = MemoryStore::with_capacity(1);
        let a = key("sweep", b"a");
        let b = key("sweep", b"b");

        store.claim(&a, Duration::from_millis(50)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(90)).await;
        // At capacity, but the sole slot is expired: the forced sweep
        // frees it instead of failing closed.
        assert!(matches!(
            store.claim(&b, Duration::from_secs(60)).await,
            Ok(Claim::First)
        ));
    }

    #[tokio::test]
    async fn complete_after_release_records_a_lapsed_response() {
        let store = MemoryStore::new();
        let k = key("orders", b"late");

        store.claim(&k, Duration::from_secs(60)).await.unwrap();
        store.release(&k).await.unwrap();
        // Complete finds no claim: records a lapsed response rather than
        // erroring, so the next claim is First (not a stale replay).
        store.complete(&k, b"orphan".to_vec()).await.unwrap();
        assert!(matches!(
            store.claim(&k, Duration::from_secs(60)).await,
            Ok(Claim::First)
        ));
    }

    #[tokio::test]
    async fn response_window_matches_claim_window() {
        let store = MemoryStore::new();
        let k = key("orders", b"window");

        store.claim(&k, Duration::from_secs(60)).await.unwrap();
        // Complete well before the claim deadline: the replay window is
        // the remaining claim window.
        store.complete(&k, b"resp".to_vec()).await.unwrap();
        assert!(matches!(
            store.claim(&k, Duration::from_secs(60)).await,
            Ok(Claim::Replay(_))
        ));
    }

    #[tokio::test]
    async fn accessors_and_minimum_capacity() {
        let store = MemoryStore::with_capacity(0);
        assert_eq!(store.capacity(), 1, "capacity minimum is one");
        assert!(store.is_empty());
        store
            .claim(&key("acc", b"one"), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
        store.sweep_expired();
        assert_eq!(store.len(), 1, "live entries survive a manual sweep");

        assert_eq!(MemoryStore::new().capacity(), DEFAULT_CAPACITY);
    }
}
