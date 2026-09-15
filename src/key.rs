//! Scoped idempotency-key derivation.

use std::fmt;

use crate::error::IdempotencyError;

/// A derived idempotency key: `"{scope}:{blake3-hex}"`.
///
/// Derivation is deterministic — the same scope and request bytes always
/// produce the same key — so independent processes (or retries after a
/// crash) converge on the same claim without sharing pre-derived secrets.
///
/// # Example
///
/// ```
/// use idempotency_kit::IdempotencyKey;
///
/// let key = IdempotencyKey::derive("orders", br#"{"amount":4200}"#)
///     .expect("valid scope");
/// assert_eq!(key.as_str().split(':').next(), Some("orders"));
/// assert_eq!(key.as_str().len(), "orders".len() + 1 + 64); // 64 hex chars
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IdempotencyKey {
    /// The full `"{scope}:{hex}"` form; also the [`Display`] form.
    rendered: String,
    /// The validated scope, kept for store namespacing.
    scope: String,
}

impl IdempotencyKey {
    /// Derive a key from a scope and the canonical request bytes.
    ///
    /// The digest is BLAKE3-256 over `request_bytes`, hex-encoded (64
    /// lowercase hex characters), prefixed with the scope. Callers should
    /// hash over a *canonical* serialization of the request (stable field
    /// order, normalized types) so semantically identical requests map to
    /// the same key.
    ///
    /// # Errors
    ///
    /// [`IdempotencyError::InvalidScope`] unless `scope` matches
    /// `[a-z0-9_.-]{1,64}`.
    pub fn derive(scope: &str, request_bytes: &[u8]) -> Result<Self, IdempotencyError> {
        if !is_valid_scope(scope) {
            return Err(IdempotencyError::InvalidScope {
                scope: scope.to_owned(),
            });
        }
        let digest = blake3::hash(request_bytes);
        let hex = hex::encode(digest.as_bytes());
        Ok(Self {
            rendered: format!("{scope}:{hex}"),
            scope: scope.to_owned(),
        })
    }

    /// The full key as stored and transported: `"{scope}:{hex}"`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.rendered
    }

    /// The validated scope this key was derived under.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// The hex-encoded BLAKE3 digest (64 lowercase hex characters).
    #[must_use]
    pub fn hex(&self) -> &str {
        self.rendered.split_once(':').map_or("", |(_, hex)| hex)
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.rendered)
    }
}

/// Scopes must match `[a-z0-9_.-]{1,64}`.
///
/// The charset keeps keys safe as Redis key fragments, URL path segments,
/// and database identifiers without escaping. The 64-character bound keeps
/// `"{scope}:{hex}"` under typical 128-byte key budgets across stores.
fn is_valid_scope(scope: &str) -> bool {
    let bytes = scope.as_bytes();
    (1..=64).contains(&bytes.len())
        && bytes
            .iter()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-'))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn derivation_is_deterministic() {
        let a = IdempotencyKey::derive("orders", b"payload-1").unwrap();
        let b = IdempotencyKey::derive("orders", b"payload-1").unwrap();
        assert_eq!(a, b);

        let other_bytes = IdempotencyKey::derive("orders", b"payload-2").unwrap();
        assert_ne!(a, other_bytes);

        let other_scope = IdempotencyKey::derive("payments", b"payload-1").unwrap();
        assert_ne!(a, other_scope);
    }

    #[test]
    fn key_format_is_scope_colon_hex() {
        let key = IdempotencyKey::derive("orders", b"payload").unwrap();
        assert!(key.as_str().starts_with("orders:"));
        let hex = key.hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')));
        assert_eq!(key.as_str(), format!("orders:{hex}"));
        assert_eq!(key.scope(), "orders");
        assert_eq!(key.to_string(), key.as_str());
    }

    #[test]
    fn different_requests_never_collide_in_practice() {
        let seen: std::collections::HashSet<String> = (0_u32..1_000)
            .map(|i| {
                IdempotencyKey::derive("probe", &i.to_le_bytes())
                    .unwrap()
                    .as_str()
                    .to_owned()
            })
            .collect();
        assert_eq!(seen.len(), 1_000);
    }

    #[test]
    fn scope_validation_accepts_charset() {
        for scope in [
            "a",
            "orders",
            "checkout-v2",
            "a.b_c-d9",
            "0",
            &"x".repeat(64),
        ] {
            assert!(
                IdempotencyKey::derive(scope, b"").is_ok(),
                "scope {scope:?} must be accepted"
            );
        }
    }

    #[test]
    fn scope_validation_rejects_the_rest() {
        let rejected = [
            "",              // empty
            "Orders",        // uppercase
            "has space",     // space
            "with/slash",    // path traversal attempt
            "with:colon",    // collides with the scope:hex separator
            "ünicode",       // non-ASCII
            "emoji🙂",       // non-ASCII
            &"x".repeat(65), // too long
        ];
        for scope in rejected {
            let err = IdempotencyKey::derive(scope, b"").expect_err("scope must be rejected");
            assert!(
                matches!(err, IdempotencyError::InvalidScope { .. }),
                "scope {scope:?} must be InvalidScope, got {err:?}"
            );
        }
    }
}
