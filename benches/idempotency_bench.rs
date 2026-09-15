//! Criterion benches: key derivation cost and the memory-store
//! claim/complete loop — the two paths applications put on their hot path.
#![cfg_attr(not(feature = "memory"), allow(unused_imports))]
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

#[cfg(feature = "memory")]
use criterion::{criterion_group, criterion_main, Criterion};
#[cfg(feature = "memory")]
use idempotency_kit::{IdempotencyExecutor, IdempotencyKey, IdempotencyStore, MemoryStore};
#[cfg(feature = "memory")]
use std::hint::black_box;
#[cfg(feature = "memory")]
use std::time::Duration;

/// BLAKE3 derivation over a 1 MiB request body — the streaming-upload
/// shape (throughput-oriented, not latency-bound).
#[cfg(feature = "memory")]
fn bench_derive_1mib(c: &mut Criterion) {
    let body = vec![0xAB_u8; 1024 * 1024];
    c.bench_function("derive_1mib", |b| {
        b.iter(|| {
            black_box(IdempotencyKey::derive(black_box("bench-scope"), black_box(&body)).unwrap())
        });
    });
}

/// Derivation over a small, typical JSON webhook body (latency path).
#[cfg(feature = "memory")]
fn bench_derive_small(c: &mut Criterion) {
    let body = br#"{"order_id":"A1","amount":4200}"#;
    c.bench_function("derive_small", |b| {
        b.iter(|| black_box(IdempotencyKey::derive(black_box("orders"), black_box(body)).unwrap()));
    });
}

/// The full single-flight cycle against the in-process store: claim →
/// complete, then a replay claim, against a fixed working set of keys.
#[cfg(feature = "memory")]
fn bench_memory_claim_complete_replay(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store = MemoryStore::new();
    let keys: Vec<IdempotencyKey> = (0..64_u32)
        .map(|i| IdempotencyKey::derive("bench", &i.to_le_bytes()).unwrap())
        .collect();
    let ttl = Duration::from_secs(3600);

    c.bench_function("memory_claim_complete_replay", |b| {
        b.iter(|| {
            rt.block_on(async {
                for key in &keys {
                    // Release leftover state so every iteration starts from
                    // the First path, then exercise the full cycle.
                    store.release(key).await.unwrap();
                    match store.claim(key, ttl).await.unwrap() {
                        idempotency_kit::Claim::First => {
                            store
                                .complete(key, b"bench-response".to_vec())
                                .await
                                .unwrap();
                        }
                        _ => unreachable!("freshly released key must claim First"),
                    }
                    match store.claim(key, ttl).await.unwrap() {
                        idempotency_kit::Claim::Replay(bytes) => {
                            black_box(bytes);
                        }
                        _ => unreachable!("completed key must replay"),
                    }
                }
            });
        });
    });
}

/// The executor end to end over the memory store (closure + claim +
/// complete), the default application shape.
#[cfg(feature = "memory")]
fn bench_executor_round_trip(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store = MemoryStore::new();
    let key = IdempotencyKey::derive("bench", b"executor-key").unwrap();
    let ttl = Duration::from_secs(3600);

    c.bench_function("executor_round_trip", |b| {
        b.iter(|| {
            rt.block_on(async {
                store.release(&key).await.unwrap();
                let response = IdempotencyExecutor::execute(&store, &key, ttl, || async {
                    Ok::<_, std::convert::Infallible>(vec![1_u8; 64])
                })
                .await
                .unwrap();
                black_box(response);
            });
        });
    });
}

#[cfg(feature = "memory")]
criterion_group!(
    benches,
    bench_derive_1mib,
    bench_derive_small,
    bench_memory_claim_complete_replay,
    bench_executor_round_trip
);
#[cfg(feature = "memory")]
criterion_main!(benches);

#[cfg(not(feature = "memory"))]
fn main() {}
