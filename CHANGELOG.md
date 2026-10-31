# Changelog

All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/): versions follow [semver](https://semver.org).

## [1.0.0] - 2026-10-24

### Declared stable

First 1.0. `idempotency-kit` guards production write paths (ledger posting,
webhook intake); this release commits the key derivation, TTL claim, response
replay, and store trait surfaces as API-stable.

### Guarantees from here on

- Breaking changes to public items require a minor bump plus a `#[deprecated]`
  window of at least one minor release.
- MSRV bumps are minor releases, not patch releases.
- `#![deny(missing_docs)]` and `#![forbid(unsafe_code)]` are enforced.
- A claim accepted by any 1.x store remains replayable by later 1.x stores.

### Explicitly not guaranteed

- The sqlite store may change its schema in minors; migrations ship with the
  release notes.
- Redis key naming may gain a version prefix in minors.

## [0.1.0] - 2026-09-15

### Added

- `IdempotencyKey::derive(scope, request_bytes)`: scoped BLAKE3-256 key
  derivation (`"{scope}:{64-hex}"`), deterministic across processes, with
  `[a-z0-9_.-]{1,64}` scope validation and a typed `InvalidScope` error.
- `IdempotencyStore` trait (object-safe via `async_trait`): atomic
  `claim` returning `Claim::{First, Replay, InFlight}`, `complete` for
  response recording, `release` for error-path rollback.
- `MemoryStore` (default `memory` feature): DashMap-backed,
  TTL-expiring, capacity-bounded (default 65,536) with fail-closed
  `StoreError::CapacityExceeded`; mirrors webhookkit 2.0.0 replay-guard
  semantics.
- `RedisStore` (`redis` feature): `SET NX EX` atomic claims, response
  replay under `{key}:resp` with the claim's remaining TTL, `DEL`
  release, connection-manager style shared with the estate, 1-second
  TTL floor.
- `IdempotencyExecutor::execute`: claim → execute → complete/release
  driver with `IdempotencyError::{Store, ConcurrentRequest,
  ExecutionFailed}`; exactly-once response replay with documented
  at-least-once execution semantics.
- Criterion benches (`derive_1mib`, `derive_small`,
  `memory_claim_complete_replay`, `executor_round_trip`), lifecycle and
  concurrency integration tests, docker-gated testcontainers Redis
  integration tests.
