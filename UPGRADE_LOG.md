# Dependency Upgrade Log

Newest entry first. Each refresh records what moved, why it is safe (release notes read, not
guessed), and which test run proves it.

---

## 2026-09-02 refresh

**Date:** 2026-09-02  |  **Project:** sqlmodel_rust  |  **Language:** Rust  |  **Manifest:** Cargo.toml (workspace, 12 published crates + `sqlmodel-e2e`)  |  **Toolchain:** nightly-2026-08-25 (pinned)

### Summary
- **Direct dependencies checked:** 27 (incl. optional TLS/console)  |  **Updated:** 3 lockfile moves + a 55-package transitive refresh  |  **Removed:** 1 (`rustls-pemfile`)  |  **Skipped by policy:** 2  |  **Failed:** 0  |  **Tracked follow-ups:** 3 audit findings (beads below)

### Baseline before any change (commit cc8e564)
- `cargo test --workspace --no-fail-fast`: 40 suites, 1960 passed, 0 failed, 185 ignored
- `cargo fmt --check`: clean  |  `cargo clippy --workspace --all-targets -- -D warnings`: clean
- `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps`: failed on two unresolved intra-doc links in `crates/sqlmodel-postgres/src/protocol/reader.rs` (bd-o59n) — fixed in this session (`Self::next_message`, `Self::feed`)
- `cargo audit`: 1 vulnerability, 3 unmaintained warnings, 1 yanked crate (see Security)

### Updates

#### asupersync: 0.4.9 → 0.4.10 (lockfile only; requirement stays `^0.4.9`)
- **Research:** asupersync CHANGELOG "v0.4.10 Release": lock-free `Cx::published_cancel_requested()` for hot cancellation polls and a RaptorQ lib-test build fix; "preserving the v0.4.3 public compatibility floor". Published 2026-09-01.
- **Breaking:** none (additive on the v0.4.3 floor)  |  **Migration:** none
- **Change:** `cargo update -p asupersync --precise 0.4.10`
- **Tests:** `cargo test --workspace --no-fail-fast` → 40 suites, 1960 passed, 0 failed, 185 ignored

#### fsqlite / fsqlite-core / fsqlite-types / fsqlite-error: 0.3.13 → 0.3.14 (lockfile + manifest)
- **Research:** FrankenSQLite v0.3.14 (GitHub Release, 19 commits since v0.3.13): FTS5 stock-compatibility writer fix (GH#404 — indexes written by older fsqlite need `INSERT INTO t(t) VALUES('rebuild')` to verify under stock SQLite; still fsqlite-readable), GH#402 checkpoint watermark (super-linear autocommit fix), `PRAGMA wal_checkpoint` cumulative-nBackfill parity, legacy-automerge origin-poisoning fix. Requires `asupersync >=0.4.3, <0.5` (compatible with 0.4.10). No API removals or renames in the 0.3.14 section.
- **Breaking:** none for the adapter surface (`AsyncConnection` open/execute/query, `SqliteValue`, `FrankenError` variants)  |  **Migration:** none in code (SQLModel creates no FTS5 tables)
- **Change:** `cargo update -p fsqlite -p fsqlite-core -p fsqlite-types -p fsqlite-error` (the 11 lockstep `fsqlite-*` internals followed); `crates/sqlmodel-frankensqlite/Cargo.toml` requirements `fsqlite = "0.3.11"`, `fsqlite-core/-types/-error = "0.3.7"` → all `"0.3.14"`, so the manifest states the version actually tested (the previous refresh, cc8e564, had moved only the lock)
- **Tests:** `cargo test -p sqlmodel-frankensqlite` → 77 passed, 0 failed, 1 ignored (env-gated strict-durable test, as before)

#### Transitive refresh (`cargo update`, 55 packages)
- **Notable:** `chacha20 0.10.1` (YANKED; via asupersync → chacha20poly1305) → 0.10.2 ("Use of SSE4.1 intrinsic in SSE2 backend of RNG and legacy variants", RustCrypto/stream-ciphers#580); `rustls-webpki 0.103.13 → 0.103.15`; `regex-automata 0.4.16 → 0.4.18`; `thiserror 2.0.19 → 2.0.20`; `smallvec 1.15.2 → 1.16.0`; `miniz_oxide 0.8.9 → 0.9.1` (adds `zlib-rs 0.6.7`, drops `arrayref`); `rand 0.8.7 → 0.8.8` (transitive line only); patch bumps of `cc`, `flate2`, `indexmap`, `log`, `lru`, `io-uring`, the `wasm-bindgen` family, `zerocopy`, `aes`/`aes-gcm`, `blake3`, `crc32fast`, `either`, `futures-*`.
- **Breaking:** none expected (all within existing requirements)
- **Tests:** covered by the FrankenSQLite run above (which rebuilt the full asupersync/fsqlite graph) and by the post-refresh workspace gate below

#### Removed: rustls-pemfile 2.2.0 (unmaintained, RUSTSEC-2025-0134)
- Replaced by `rustls::pki_types::pem::PemObject` (rustls-pki-types 1.15.1, already in the graph) in `crates/sqlmodel-mysql/src/tls.rs` (`read_pem_certificates`, `read_pem_private_key`); the `tls` feature no longer pulls the crate. Eight PEM fixture tests (CA bundle with comments/CRLF, PKCS#8/PKCS#1/SEC1 keys, cert-where-key-expected, encrypted key rejected, invalid base64, missing END line) run under `cargo test -p sqlmodel-mysql --features tls`. Bead bd-j7wt.3.

### Skipped (policy)

#### rsa: stays at 0.10.0-rc.18
- **Reason:** the only stable line is 0.9.x (0.9.10), built on `digest 0.10`/`rand_core 0.6`, incompatible with `sha1`/`sha2` 0.11, `hmac`/`pbkdf2` 0.13 and `rand` 0.10 used by the auth code. The workspace moved to the 0.10 release-candidate line in c4bcc61; rc.18 is the newest. Move to plain `"0.10"` when the stable release ships (Cargo.toml comment already says so).
- **Audit:** RUSTSEC-2023-0071 still reported (no fixed version). Non-reachability analysis (client encrypts with the server's public key only; Marvin targets private-key decryption) recorded in `.cargo/audit.toml`, `deny.toml`, and `crates/sqlmodel-mysql/README.md`; a source-scan test in `auth.rs` fails if a private-key API is ever introduced. Beads bd-j7wt.1 (done in-session) / bd-j7wt.2 (feature-gate the RSA path).

#### rustls: stays at 0.23.43
- **Reason:** latest stable; 0.24.0-dev.1 is a pre-release.

#### Already at latest stable (no action)
serde 1.0.229, serde_json 1.0.151, regex 1.13.1, tracing 0.1.44, rich_rust 0.2.3, proc-macro2 1.0.107, quote 1.0.47, syn 3.0.4, sha1 0.11.0, sha2 0.11.0, hmac 0.13.0, pbkdf2 0.13.0, rand 0.10.2, md5 0.8.1, libsqlite3-sys 0.38.2, base64 0.23.1, subtle 2.6.1, webpki-roots 1.0.9.

### Security (`cargo audit` / `cargo deny check`)
- `cargo deny check advisories bans licenses sources` → all four ok (new `deny.toml`: bans the AGENTS.md forbidden crates — tokio, hyper, reqwest, axum, tower, async-std, smol, sqlx, diesel, sea-orm, rusqlite — and clarifies the fsqlite crates' missing `license` field as the same MIT-with-rider text).
- `cargo audit --deny warnings` → clean with the documented ignores in `.cargo/audit.toml`:
  - rsa RUSTSEC-2023-0071 (analysis above; review 2026-12-01)
  - bincode RUSTSEC-2025-0141 and yaml-rust RUSTSEC-2024-0320: reach the graph only via `syntect` default features ← `rich_rust` ← `sqlmodel-console` (`rich`); used for syntect's bundled syntax dumps, never for input SQLModel receives; fix belongs in rich_rust (bd-j7wt.4); review 2026-11-01
- chacha20 0.10.1 yank resolved by this refresh (0.10.2).
- CI's Security job now fails on findings (`cargo audit --deny warnings` + `cargo deny check`) instead of `cargo audit || true`; it gates the release build job.

### Post-refresh gates (final tree of the 2026-09-02 session)
- `cargo fmt --all --check` clean; `cargo clippy --workspace --all-targets -- -D warnings` clean; `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps` clean.
- `cargo test --workspace --no-fail-fast`: 61 suites, 2041 passed, 0 failed, 329 ignored (pre-existing); `cargo test --workspace --doc`: 52 passed.
- `cargo audit --deny warnings` clean; `cargo deny check advisories bans licenses sources` all ok.
- Live databases (local Docker): postgres:16 and mysql:8.4 integration suites 7/7 each; `sqlmodel-e2e` smoke/migrations/concurrent-writers green on C SQLite, FrankenSQLite, PostgreSQL, MySQL. The refreshed asupersync 0.4.10 / fsqlite 0.3.14 lockfile is what these ran on.

### Commands used
```bash
cargo update -p asupersync --precise 0.4.10
cargo update -p fsqlite -p fsqlite-core -p fsqlite-types -p fsqlite-error
cargo update                                   # transitive refresh (55 packages)
cargo test --workspace --no-fail-fast          # after step 1
cargo test -p sqlmodel-frankensqlite           # after steps 2 + 3
cargo audit --deny warnings && cargo deny check advisories bans licenses sources
```
Builds were invoked as `rch exec -- cargo ...` because the RCH PreToolUse hook refused plain cargo commands on this machine (`force_remote = true`, fleet pressure); see AGENTS.md "RCH".

### Notes
- GitHub Actions versions were not reviewed in this refresh; tracked as bd-qz1a.4.
- The 0.4.2 release is half-published on crates.io (only `sqlmodel-core` and `sqlmodel-frankensqlite`); this refresh changes `Cargo.lock` (not published for libraries) and the frankensqlite/mysql manifest requirements only, so registry consumers are unaffected. See bd-jeof.1.

---

## 2026-02-19 refresh

**Date:** 2026-02-19  |  **Project:** sqlmodel_rust  |  **Language:** Rust

### Summary
- **Updated:** 3  |  **Skipped:** 1  |  **Failed:** 0  |  **Needs attention:** 0

### Updates

#### syn: 2.0.114 → 2.0.116
- **Breaking:** None (patch release)
- **Change:** `cargo update -p syn` (version spec `"2"` already allows it)
- **Tests:** Pass (161 tests in sqlmodel-macros)

#### md5: 0.7.0 → 0.8.0
- **Breaking:** `Context::compute()` deprecated in favor of `Context::finalize()`. Added `no_std` support.
- **Migration:** None needed — project only uses `md5::compute()` free function (unchanged API)
- **Change:** `md5 = "0.7"` → `md5 = "0.8"` in sqlmodel-postgres/Cargo.toml
- **Tests:** Pass (59 unit + 7 integration tests in sqlmodel-postgres)

#### webpki-roots: 0.26.11 → 1.0.6
- **Breaking:** None (API identical; 0.26.11 already re-exported 1.0 via semver trick)
- **Migration:** None needed — `TLS_SERVER_ROOTS` constant unchanged
- **Change:** `webpki-roots = "0.26"` → `webpki-roots = "1"` in sqlmodel-postgres and sqlmodel-mysql
- **Tests:** Pass (all postgres + mysql tests)

### Skipped

#### rand: 0.8.5 → 0.10.0
- **Reason:** `rsa` 0.9.x (stable) depends on `rand_core` 0.6, which is incompatible with `rand` 0.10 (`rand_core` 0.10). The only compatible `rsa` version is 0.10.0-rc.15 (pre-release). Per policy, we do not upgrade to pre-release versions.
- **Action:** Revisit when `rsa` 0.10.0 stable is released. (Superseded 2026-07: the workspace moved to the rsa 0.10 rc line together with rand 0.10; see the 2026-09-02 entry.)
- **Affected crates:** sqlmodel-postgres (auth/scram.rs), sqlmodel-mysql (auth.rs)

### Pre-Existing Issues

#### sqlmodel-schema test failure (unrelated)
- `create::tests::test_create_table_sql_type_override` fails on both old and new dependency versions
- Not caused by any dependency update
