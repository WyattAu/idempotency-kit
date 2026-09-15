//! Idempotency keys for Rust — scoped key derivation, TTL claims with
//! response replay, and pluggable stores.
//!
//! `idempotency-kit` unifies the replay/claim primitive that previously
//! existed separately in webhookkit's replay guard and the estate's
//! Redis-backed request deduplication: one [`IdempotencyStore`] contract
//! with atomic claims, TTL-bounded windows, and response replay, so a
//! retried request returns the original response instead of executing
//! twice.
//!
//! # Design
//!
//! - **Deterministic derivation.** [`IdempotencyKey::derive`] hashes
//!   canonical request bytes with BLAKE3 into
//!   `"{scope}:{64-hex-chars}"`; every process derives the same key from
//!   the same request without coordination.
//! - **Atomic claims.** [`IdempotencyStore::claim`] is a compare-and-set:
//!   exactly one concurrent caller (per key, across processes for the
//!   Redis store) observes [`Claim::First`]; everyone else observes
//!   [`Claim::Replay`] or [`Claim::InFlight`].
//! - **Fail-closed capacity.** The in-process `MemoryStore` rejects new
//!   claims at capacity rather than evicting live ones — dropping a claim
//!   would re-open the duplicate-execution window the kit exists to
//!   close.
//! - **Exactly-once response replay, at-least-once execution.** The kit
//!   guarantees a repeated request gets the same response bytes within
//!   the TTL window; side effects must still be idempotent across TTL
//!   boundaries. See [`IdempotencyExecutor`] for the full contract.
//!
//! # Example
//!
//! ```
//! # #[cfg(feature = "memory")] fn main() {
//! #     let rt = tokio::runtime::Builder::new_current_thread()
//! #         .enable_all()
//! #         .build()
//! #         .unwrap();
//! #     rt.block_on(async {
//! #         demo().await.ok();
//! #     });
//! # }
//! # #[cfg(feature = "memory")]
//! # async fn demo() -> Result<(), idempotency_kit::IdempotencyError> {
//! use idempotency_kit::{IdempotencyExecutor, IdempotencyKey, MemoryStore};
//!
//! let store = MemoryStore::new();
//! let key = IdempotencyKey::derive("orders", br#"{"order_id": "A1"}"#)?;
//!
//! let first = IdempotencyExecutor::execute(&store, &key, std::time::Duration::from_secs(300), || async {
//!     // Charge the card, provision the resource, ...
//!     Ok::<_, std::convert::Infallible>(b"{\"status\":\"charged\"}".to_vec())
//! }).await?;
//!
//! // A retry (network blip, double-click, at-least-once delivery)
//! // replays the recorded response without executing again.
//! let replay = IdempotencyExecutor::execute(&store, &key, std::time::Duration::from_secs(300), || async {
//!     // If this ran, the sentinel would leak into the result.
//!     Ok::<_, std::convert::Infallible>(b"sentinel: closure ran".to_vec())
//! }).await?;
//! assert_eq!(first, replay);
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "memory"))]
//! # fn main() {}
//! ```
//!
//! # Stores
//!
#![cfg_attr(
    feature = "memory",
    doc = "| [`MemoryStore`] | `memory` (default) | process-local | single-instance services, tests |"
)]
#![cfg_attr(
    feature = "redis",
    doc = "| [`RedisStore`] | `redis` | shared across processes | multi-instance deployments |"
)]
//!
//! | Store | Feature | Consistency | Use for |
//! |---|---|---|---|
//!
//! Any deployment can bring its own store by implementing
//! [`IdempotencyStore`] (e.g. Postgres `INSERT ... ON CONFLICT`).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod executor;
mod key;
mod store;

pub use error::{IdempotencyError, StoreError};
pub use executor::IdempotencyExecutor;
pub use key::IdempotencyKey;
pub use store::{Claim, IdempotencyStore};

#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "memory")]
pub use memory::{MemoryStore, DEFAULT_CAPACITY};

#[cfg(feature = "redis")]
mod redis_store;
#[cfg(feature = "redis")]
pub use redis_store::RedisStore;
