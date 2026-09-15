# idempotency-kit

Idempotency keys for Rust — the shared request-deduplication pattern of
the WyattAu estate, unified from webhookkit's replay guard and the
Redis-backed request deduplication in the commerce services.

- **Deterministic key derivation**: scoped BLAKE3 — the same request bytes
  always derive the same `"{scope}:{hex}"` key on every worker, with no
  coordination.
- **Atomic TTL claims with response replay**: exactly one concurrent caller
  executes; everyone else replays the recorded response or fails fast with
  a typed `ConcurrentRequest` error.
- **Pluggable stores**: in-process `MemoryStore` (default) and
  process-shared `RedisStore` (`SET NX EX`, no Lua), behind one
  object-safe `IdempotencyStore` trait — bring your own (Postgres,
  DynamoDB, …) for any other deployment.
- **Fail-closed capacity**: the memory store rejects new claims at
  capacity rather than evicting live ones.
- **`#![forbid(unsafe_code)]`, `#![deny(missing_docs)]`**, clippy
  `unwrap_used`/`expect_used`/`panic`/`indexing_slicing` denied.

## Install

```toml
[dependencies]
idempotency-kit = "0.1"
```

## Example

```rust
use idempotency_kit::{IdempotencyExecutor, IdempotencyKey, MemoryStore};

let store = MemoryStore::new();
let key = IdempotencyKey::derive("orders", br#"{"order_id":"A1"}"#).unwrap();

let first = IdempotencyExecutor::execute(&store, &key, std::time::Duration::from_secs(300), || async {
    // Charge the card, provision the resource, ...
    Ok::<_, std::convert::Infallible>(b"{\"status\":\"charged\"}".to_vec())
}).await.unwrap();

// The retry (double-click, at-least-once webhook delivery, network blip)
// replays the recorded response without executing again.
let replay = IdempotencyExecutor::execute(&store, &key, std::time::Duration::from_secs(300), || async {
    Ok::<_, std::convert::Infallible>(b"sentinel: closure ran".to_vec())
}).await.unwrap();
assert_eq!(first, replay);
```

Multi-instance deployments share claims through Redis:

```toml
[dependencies]
idempotency-kit = { version = "0.1", features = ["redis"] }
```

```rust,ignore
let conn = redis::Client::open("redis://internal:6379/")?
    .get_connection_manager().await?;
let store = idempotency_kit::RedisStore::new(conn);
```

## Semantics & precedence

The kit provides **exactly-once response replay**, not exactly-once
execution. Read the precedence rules before wiring it into a payment
path:

1. **Claims are atomic and TTL-bounded.** `claim` is a compare-and-set:
   one caller observes `Claim::First`; others observe `Claim::Replay`
   (a completed response exists) or `Claim::InFlight` (someone is
   mid-request). The executor surfaces `InFlight` as
   `IdempotencyError::ConcurrentRequest` — it never queues or waits; the
   caller retries or collapses duplicates upstream.
2. **At-least-once execution across TTL boundaries is the caller's
   concern.** A request that outlives its claim TTL can execute again on
   retry. Make side effects idempotent (natural keys, conditional
   writes); the kit guarantees the *response*, not the side effect.
3. **Errors release; successes replay.** A failing closure releases its
   claim so a retry executes; a succeeding closure records its response,
   which replays until the window lapses. The response window equals the
   claim window (`complete` does not extend it).
4. **Capacity fails closed.** At capacity with only live claims,
   `MemoryStore` returns `StoreError::CapacityExceeded` instead of
   evicting: forgetting a held claim would re-admit a duplicate
   execution — the exact failure the kit exists to prevent. Shed load
   instead.
5. **Zero TTLs are floored to 1 second** (`RedisStore`) so no store can
   produce instant-expire claims.

### Key derivation

`IdempotencyKey::derive(scope, request_bytes)` = `"{scope}:" ++ hex(BLAKE3-256(request_bytes))`.

- **Why BLAKE3?** Keys are identifiers, not secrets — no keyed MAC is
  needed, so BLAKE3's speed (~GB/s, SIMD/parallel) beats HMAC-SHA256 at
  zero security cost here. The 256-bit digest gives collision/preimage
  margins that make deliberate cross-customer key collisions
  infeasible, and the dependency builds with `default-features = false`
  (no std requirement in the hash path).
- **Scopes namespace claims** (`[a-z0-9_.-]{1,64}`): the same request
  bytes under `"orders"` and `"refunds"` never share a claim, and the
  charset keeps keys safe as Redis fragments and URL segments.
- **Hash canonical bytes.** Serialize requests with stable field order
  and normalized types before deriving, so semantically identical
  requests map to one key.

## Performance

Measured with criterion on the committed bench suite (`cargo bench`);
see `benches/idempotency_bench.rs`:

| Operation | Path | Cost model |
|---|---|---|
| `IdempotencyKey::derive` (1 MiB) | hot | BLAKE3 streaming throughput |
| `IdempotencyKey::derive` (small body) | hot | one BLAKE3 permutation pass + hex |
| `claim → complete → replay` (memory) | hot | sharded map ops + one clone per replay |
| `executor` round trip (memory) | hot | claim + closure + complete |

Publish your measured numbers per the estate standard; the committed
suite is the shared methodology.

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `memory` | yes | `MemoryStore`: DashMap-backed, capacity-bounded, process-local |
| `redis` | no | `RedisStore`: `SET NX EX` claims, shared across instances |

## Integration tests (redis)

The Redis integration tests run against a throwaway testcontainers
Redis and are `#[ignore]`-gated on docker availability:

```sh
cargo test --features redis --test redis_idempotency -- --include-ignored
```

CI runs them on every push (GitHub runners have docker).

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT)
at your option.
