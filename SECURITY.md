# Security Policy — idempotency-kit

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |

## Reporting a vulnerability

Report privately via [GitHub security advisories] for this repository, or
email **wyatt_au@protonmail.com**. Do **not** open a public issue for
security reports.

You will receive an acknowledgement within **72 hours**. Coordinated
disclosure: we ask for up to 90 days before public disclosure while a
patch ships.

## Scope notes

`idempotency-kit` guards request execution with TTL-bounded claims.
Security considerations for integrators:

- **Keys are identifiers, not secrets.** Derivation is unkeyed BLAKE3:
  anyone who can compute the canonical request bytes can compute the
  key. Do not embed anything in a key you would not publish; protect
  the *claim endpoints*, not the key format.
- **Exactly-once response replay, at-least-once execution.** The kit
  does not make side effects idempotent — a request that outlives its
  TTL claim can execute twice. Bound your TTLs beyond worst-case
  request duration and make side effects idempotent at the source
  (natural keys, conditional writes).
- **Scope validation is the namespace boundary.** Scopes are restricted
  to `[a-z0-9_.-]{1,64}` so one service cannot collide with another
  service's claims on shared Redis infrastructure. Derive keys with
  distinct scopes per trust domain.
- **Fail-closed capacity.** The memory store rejects new claims at
  capacity rather than evicting live ones. A capacity error is a
  load-shedding signal, not a bug to route around.
- **Redis is part of your trust boundary.** The store assumes an
  authenticated, encrypted connection (`redis://` with ACLs / TLS via
  your deployment); claim keys are readable by anyone with Redis
  access, and responses may contain application data.
- `#![forbid(unsafe_code)]` — no unsafe blocks exist in this crate.

[GitHub security advisories]:
    https://github.com/WyattAu/idempotency-kit/security/advisories/new
