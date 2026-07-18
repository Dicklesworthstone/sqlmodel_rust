# Changelog

All notable changes to the SQLModel Rust project are documented in this file.

This project follows approximate [Semantic Versioning](https://semver.org/). Versions below 1.0.0 may include breaking API changes in minor releases.

Repository: <https://github.com/Dicklesworthstone/sqlmodel_rust>

---

## [0.3.0] -- 2026-07-18

**Contention-safe pooled FrankenSQLite teardown.** Pool retirement now has an
explicit driver hook, allowing FrankenSQLite connections to skip the final WAL
checkpoint without weakening durability. This is a pre-1.0 minor release
because the new method extends the public `Connection` trait.

### Added

- Add `Connection::close_for_pool`, with a default implementation that retains
  existing driver behavior and a documented lifecycle covering shutdown,
  idle/max-lifetime eviction, and failed checkout validation.
- Route every normal pool-retirement path through the hook after removing the
  connection from pool state and releasing the internal mutex.
- Add `FrankenConnection::close_without_checkpoint_sync` and use it for pooled
  FrankenSQLite retirement. Committed WAL frames remain durable and are
  recovered on the next open.
- Add focused coverage for shutdown, eviction, validation failure, unlocked
  callback execution, and a real close/reopen durability round trip.

### Changed

- Update the exact `asupersync` dependency from 0.3.5 to 0.3.9.
- Migrate the shared PostgreSQL and MySQL wrappers to asupersync's owned mutex
  guard, preserving `Send` futures across network awaits under the 0.3.9
  non-`Send` borrowed-guard contract.
- Update the FrankenSQLite driver crates to 0.1.17 and exercise their published
  registry artifacts in the release gate matrix.
- Remove obsolete local FrankenSQLite patches so a standalone checkout resolves
  the same registry graph as published consumers without requiring a sibling
  repository.
- Clear newly enforced release-toolchain lints in immediate async trait
  implementations, debug assertions, time arithmetic, and formatting so the
  workspace remains warning-free.
- Make repeated FrankenSQLite file tests deterministic by clearing the WAL and
  shared-memory sidecars alongside the primary database.
- Correct package and README repository links to the canonical
  `Dicklesworthstone/sqlmodel_rust` repository.

Thanks to [@lifeofgurpreet](https://github.com/lifeofgurpreet) for identifying
the close-time checkpoint contention and proposing the pool-specific teardown
direction in [PR #21](https://github.com/Dicklesworthstone/sqlmodel_rust/pull/21).

## [0.2.2] -- 2026-04-23

**Windows SQLite linking fix.** `sqlmodel-sqlite` now bundles the SQLite amalgamation via `libsqlite3-sys` and pins the rlib with an explicit `#[link(name = "sqlite3", kind = "static")]` attribute, so downstream crates that don't import a symbol from `libsqlite3-sys` no longer see the rlib elided and the static-link directives dropped at link time.

### Fixed -- SQLite driver

- Always bundle `libsqlite3` via `libsqlite3-sys` + feature `bundled` instead of dynamically linking the host `sqlite3` ([`50e1aca`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/50e1aca)). Fixes `LNK1181: cannot open input file 'sqlite3.lib'` on Windows MSVC and `__memcpy_chk` unresolved externs when cross-linking to musl, and removes the silent dependency on a host `libsqlite3-dev` package on GNU Linux.
- Declare `#[link(name = "sqlite3", kind = "static")]` explicitly in `src/ffi.rs` so rustc does not elide the `libsqlite3-sys` rlib when the crate imports no items from it ([`c8bb7d7`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/c8bb7d7)). Without this, the build-script link directives were silently dropped and downstream Windows builds still failed with `__imp_sqlite3_*` unresolved symbols.

Fixes [pi_agent_rust#55](https://github.com/Dicklesworthstone/pi_agent_rust/issues/55) (Windows binary missing from releases because link step fails).

---

## [0.2.1] -- 2026-03-21 (Unreleased)

**Dependency refresh and FrankenSQLite compatibility.** Workspace bumped to 0.2.1 with updated async runtime and SQLite bindings. No new GitHub Release yet.

Bump commit: [`21b872d`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/21b872df6696f25245feeafa5f0f526cd774aca3)

### Added

- `Pool::clear_idle()` method to drain idle connections on demand ([`56cf304`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/56cf304b190b2aeac74c8cff36517ab4511babb4)).

### Fixed -- PostgreSQL driver

- Prevented a hang in `PgAsyncConnection` caused by `feed()` back-pressure; switched to `push()` for message sending ([`f82770e`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/f82770e57407afcfc1f659d00799b01670b9780c)).

### Fixed -- FrankenSQLite driver

- Adapted value conversions to `SqliteValue`'s new `Arc<str>` / `Arc<[u8]>` inner types ([`ac1ddb1`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/ac1ddb1bcff52edf677856204ff08a9d7053cc36)).
- Used `to_string()` instead of `clone()` for `SqliteValue::Text` to satisfy the updated API ([`3a3f078`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/3a3f0783a9869c5fce12d5d7530a69088016958a)).
- Implemented proper `close_sync` for `FrankenConnection` so connections release resources correctly ([`db73fb4`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/db73fb4c7a6e22b76f474a63406ed49d754cbad2)).

### Fixed -- Schema

- Auto-increment primary-key schema generation corrected after dependency upgrade ([`6a3376b`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/6a3376b24ed41ad7f1e2bb0d24980a5ab97e5aeb)).

### Changed

- Updated `asupersync` to 0.2.9 ([`cecd0c8`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/cecd0c8e7d475fa42b8557465e8c2cac492a4484)) and `frankensqlite` to 0.1.2.
- Updated `rich_rust` dependency from pre-release git ref to crates.io v0.2.0 ([`7f89759`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/7f8975922cf8e054008ced008393269980f581ac)).
- Switched FrankenSQLite (`fsqlite`) dependencies from local paths to crates.io registry 0.1.0 ([`b2086cf`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/b2086cfe8046f1d3b86f548e575350d7aa5833f2)).
- Updated license to MIT with OpenAI/Anthropic Rider ([`4c9a331`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/4c9a33194712d2a014b63c6b1b2035730cc1df6c)).

### Housekeeping

- Added GitHub social preview image ([`ef36446`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/ef364462c3762593360073f140e4befa305612fb)).
- Removed leftover `eprintln!` debug statements from FrankenSQLite connection layer ([`97e6f32`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/97e6f32d2ef88c5aed2f9e452d1d86e0e91fba6e)).
- Feature-gated SQLite integration tests; fixed PRAGMA column inference ([`31002fe`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/31002fe92b039d2be3ad76b8863fc05de53cb633)).

---

## [0.2.0] -- 2026-02-15 (GitHub Release)

**First tagged release.** 11 publishable crates providing an intuitive, type-safe SQL toolkit for Rust, all published to crates.io. Contains all features from 0.1.0 and 0.1.1 plus substantial new work on table inheritance, session improvements, schema introspection, and a pure-Rust SQLite driver.

Release: <https://github.com/Dicklesworthstone/sqlmodel_rust/releases/tag/v0.2.0>
Tag commit: [`bb1980e`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/bb1980e949a3432d7cd0fb44ec90dd4d73b6b983)

### Published crates (all at 0.2.0)

| Crate | Role |
|-------|------|
| `sqlmodel` | Facade -- re-exports the full stack for `use sqlmodel::prelude::*` |
| `sqlmodel-core` | Core traits (`Model`, `Connection`), types (`Value`, `Row`, `Error`) |
| `sqlmodel-macros` | `#[derive(Model)]` proc macro with attribute parsing and code generation |
| `sqlmodel-query` | Type-safe query builder with multi-dialect SQL generation |
| `sqlmodel-schema` | DDL generation, schema builder, migration runner |
| `sqlmodel-session` | Unit-of-work pattern with identity map |
| `sqlmodel-pool` | Connection pooling with asupersync channels |
| `sqlmodel-postgres` | PostgreSQL wire protocol driver with SCRAM-SHA-256 auth and optional TLS |
| `sqlmodel-mysql` | MySQL wire protocol driver with optional TLS |
| `sqlmodel-sqlite` | SQLite driver (FFI-based) |
| `sqlmodel-console` | Rich terminal output with agent auto-detection |

### Unpublished crate

- `sqlmodel-frankensqlite` -- pure-Rust SQLite driver built on FrankenSQLite. Depends on `frankensqlite` crates not yet on crates.io; excluded from the release publish.

### Added since 0.1.1 -- Table inheritance

- **Single-table inheritance (STI):** discriminator column support, discriminator filtering automatically applied to SELECT queries ([`4f3de74`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/4f3de7491377bb36dc46612cabee2efff9bf6fbc), [`86143a8`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/86143a8c1e2a3aef2470f79bb6e0a47252353b77)).
- **Joined-table inheritance:** parent field embedding, automatic JOINs in all SELECT/INSERT/UPDATE/DELETE paths, polymorphic queries with SQLite hydration tests, and `insert_many` support ([`f76f999`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/f76f999eade496ca1f8bb840e60242f6a020717e), [`4bcf5f1`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/4bcf5f13b161ef736f34de0829bf5244aabe5472), [`f70720e`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/f70720e88ef8c76744b72503bcac00ef939c6878), [`2ffbe04`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/2ffbe04ad3c0cfc9b48735b9b7c1c21224993fb9)).
- Explicit DML semantics for joined inheritance: inserts/updates route to the correct base+child tables ([`d9ed0a9`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/d9ed0a976c2fc710b462f93b02588c7e2f1807e9)).

### Added since 0.1.1 -- Session and ORM

- Cascade delete and orphan tracking in `Session::flush` with composite-key support ([`036d057`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/036d05799c9f089afbd77148788c0c09fab6c2a5), [`20ebdeb`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/20ebdeb23cccc37799326a049ee5ff393efd94be)).
- Composite many-to-many link table keys ([`6ce8c52`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/6ce8c5240af0c9b094732922aa7fc836e70dfb2a)).
- One-to-many batch loader for `RelatedMany` ([`3e93189`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/3e931898fccd416045fc6a5a8af397a3ea9b5fee)).
- `Select::one` with exact-one semantics and `Select::one_or_none` with strict cardinality ([`e7312c9`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/e7312c92bd11ddcf753432eee1ae168934923744), [`891b494`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/891b4942fe39ded8834b1dc01fa7a9b92b604e34)).
- `DumpOptions` python/round_trip dump modes for model serialization ([`9993f0d`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/9993f0df623199088808602d296f093b0cc22850)).
- Tracked `model_dump` with `exclude_unset` support ([`576a696`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/576a69695cf3a313b360277e15c7e313a0bf0d35)).
- Eager select aliases related columns; MySQL binary temporal decode ([`84fb123`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/84fb123f95f824571ac76261ec964f5b5c391815)).
- ORM `Session` exposed in the facade crate prelude ([`eaba0d3`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/eaba0d3ae609416a11c6cd50c0072fda3bbb25d4)).

### Added since 0.1.1 -- Schema and introspection

- Safe column dropping for SQLite < 3.35 via table recreation strategy ([`3968943`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/396894326d2e75470d7015c1c953c28d06511404)).
- SQLite ALTER COLUMN via table recreation ([`10e248a`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/10e248a7aa1e90739eb2218f944e93347878736a)).
- Introspection of SQLite CHECK constraints ([`e5ab407`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/e5ab4074100b7b55a808032dcca4c06275f433b2)).
- Postgres/MySQL introspection of CHECK constraints and comments ([`a03871e`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/a03871e81ea58fae875c101306e3b91712150a9f)).
- `current_schema` usage for Postgres introspection scope ([`fce6235`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/fce623547021a80681e3c7f3fab8d56770c7003e)).
- Driver introspection parity tests across all three dialects ([`fb30cc9`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/fb30cc9887c8d6795ff7fedb0eee0197228a6023)).

### Added since 0.1.1 -- Query builder

- MySQL `ON CONFLICT` translation ([`3750b58`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/3750b585de8860df4465e1fa062ad84250941067)).
- Quote-aware `adjust_placeholder_indices` for Postgres and SQLite dialects ([`3271821`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/32718213fbd364952936ca47ea76fd2309e56d12)).
- Dialect-aware identifier quoting extracted into shared helpers ([`2a5115f`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/2a5115f732db19d5ec0381005da2164e29919bac)).

### Added since 0.1.1 -- Drivers

- SQLite online backup API support ([`982b7f4`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/982b7f40455e8f4c1f253ef02ab4539ea8540de9)).
- New `sqlmodel-frankensqlite` crate: pure-Rust SQLite driver via FrankenSQLite with `RETURNING *` column name resolution and `BEGIN CONCURRENT` support ([`cd11cdd`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/cd11cddb5de24997b6d917c03cfb2c094ab80863), [`74bbad7`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/74bbad7348d4dde5e65323d34631793c5d33103e), [`26df685`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/26df685665367707cb8b1e4ed3a808f9d50f0401)).

### Fixed since 0.1.1

- Postgres/MySQL composite index key ordering preserved correctly ([`ce4271c`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/ce4271c0fc103f621ba93adaa2eed19fbdb5e05c), [`0a7ae9f`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/0a7ae9f4d7ad233059844a38fa90047120bccfac)).
- Console rendering hardened for narrow terminals and Unicode: `ErrorPanel`, `MigrationStatus`, `PoolStatusDisplay` all made width-safe ([`688e173`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/688e173530bfb79383fbc2c8142e74f0d661dc12), [`e05a3ee`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/e05a3ee499fe53d2599341c207cae23b0b242d69), [`6ecf4b7`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/6ecf4b777e2f9c82ff9c5f2e5977ec2f9a90d105)).
- `strip_markup` now preserves bracketed identifiers ([`c212ed0`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/c212ed0209584a9fede4b1eefeccfe831e330ca3)).
- STI child table names and discriminator propagation corrected ([`984d78d`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/984d78dbd19f7b749097038de1a167872cb3ea9e)).
- SQLite constraint DDL and unique introspection fixed ([`cc84e79`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/cc84e7936f49d0d698bb45d602c52cba2a15e1f3)).
- Introspector identifier quoting and binding corrected ([`c367807`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/c367807fae51008a0ec78511a69f1ee6c6694198)).
- Word boundary detection for SQL keyword matching in FrankenSQLite parser ([`24a86d8`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/24a86d88a209157e76c9ed76e29e33bec26733b8), [`8e06a22`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/8e06a22cfe254f92f305d2d13224653c755e7a2a)).
- Auth/TLS gaps closed in Postgres driver; real prepared statement support tightened ([`0c0b747`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/0c0b747ebd6ce1d2df43cd25618992ffba2b488a)).
- Dialect propagated correctly in `EXISTS` and lateral subqueries ([`80df00b`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/80df00bd8bb673ea11775eb9e64df075ca624776)).
- Removed `panic!` calls from cache examples, tests, and macro parse tests ([`6714f28`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/6714f284d41d916c5d6642041a6f200e31768603), [`31131e4`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/31131e47b19280b7d5898a9e786fc6d5e01722ef)).
- MySQL text temporal values parsed into structured `Value` types ([`952051b`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/952051b2dd92d0957d2c1ba2b677660ee3ad5dcb)).
- Schema-diff: refreshed SQLite recreate-op `table_info` handling ([`3d9c997`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/3d9c997e9f8ee3fcb49b17279b1a7ce5cc2512be)).
- SQLite default inserts and dialect-aware bulk insert fixed ([`6e54fc9`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/6e54fc95e3b92f087ca87bf7cb5326fc3c85111c), [`218d16f`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/218d16f8160e096b5a929dd84fcc2dfc133641ad)).

---

## [0.1.1] -- 2026-02-04 (Workspace version bump, no GitHub Release)

**crates.io preparation release.** Switched from git-sourced `asupersync` to crates.io, added workspace version management, and set up CI-driven crate publishing.

Bump commit: [`d14218b`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/d14218b21b424a7c7009215a60eccc2acf0b7191)

### Changed

- Switched `asupersync` dependency from git to crates.io ([`31cd019`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/31cd0194fc409a883582b9187c390ea676d7a7cd)).
- Added unified workspace version management in root `Cargo.toml` ([`3b33fd6`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/3b33fd651d3067f238414f304701aa987118edd3)).
- Centralized dialect helpers into a shared module ([`c9c3607`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/c9c3607d33f699b36cf19d6be576f348c68aadf4)).
- Prepared all crates for crates.io publishing with metadata, categories, and keywords ([`02e0bf0`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/02e0bf09dcb35c4893f0279e94381114ee5a2541)).

### Added

- CI workflow for automatic crate publishing on GitHub Release ([`7d9e3de`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/7d9e3de25888cb84aeaca78d6d03beae35756ef5)).
- `query_sync` and `execute_sync` exposed as public methods on the SQLite driver ([`2eab6b3`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/2eab6b30c7fd448b4e436ed59b5f107b6537c01c)).

---

## 0.1.0 -- 2026-01-17 through 2026-02-02 (Initial development, no tag or release)

**Ground-up implementation** of the entire SQLModel Rust workspace. Development began 2026-01-17 with commit [`37e6a0c`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/37e6a0c8e2384edf9475185f5d20bbe7c13867e2) and reached feature-complete status by late January before the 0.1.1 publish-preparation bump. Features below are organized by capability area.

### Model system and derive macros (sqlmodel-core, sqlmodel-macros)

- `Model` trait with `FieldInfo`, `Value`, `Row`, `Connection`, and `Error` types ([`37e6a0c`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/37e6a0c8e2384edf9475185f5d20bbe7c13867e2)).
- `#[derive(Model)]` with full attribute parsing: `primary_key`, `auto_increment`, `unique`, `nullable`, `column`, `sql_type`, `default`, `foreign_key`, `index`, `skip` ([`52ae2af`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/52ae2afc9a176742e5d894ab9a3118e8cb6587f9)).
- `#[derive(Validate)]` macro for field-level validation ([`254f22e`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/254f22edaabadf95dd8acdefbff58892b4222cd4)).
- `#[derive(SqlEnum)]` for enum-to-SQL mapping ([`32aa503`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/32aa503cd3b6c98a520295c043c4012ec398133b)).
- Built-in validators (length, range, regex, etc.) ([`93fc23e`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/93fc23ed816abfa6de4b2a1e9d2c982b0cd3ae7a)).
- Model-level validators with cross-field logic ([`babcfff`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/babcfffbb4746dbc281532e9e90823e379d7b4c4)).
- `on_delete` / `on_update` foreign-key cascade attributes ([`f50b704`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/f50b70452c5a8a1f9536cc15641825bc7aea78df)).
- `discriminator` attribute for union/polymorphic type support ([`67801c5`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/67801c54e99d69c3a56598cec20f947ba07d4736)).
- `shard_key` attribute for horizontal sharding ([`3daa9ad`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/3daa9ad65fc9f2fbeb98326d815f1998474239ea)).
- `sa_column` and `sa_relationship` full override support for SQLAlchemy compatibility ([`71cbd59`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/71cbd59402a624d74ddc8344efbec4e7575489b9), [`77ca144`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/77ca1440732018bd6470a1ad7a94d6a9cade01e1)).
- `passive_deletes` for relationship cascade control ([`5502ca2`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/5502ca2072eea8fe58fae83bb3bdd93fe56bc193)).
- Computed field support via `#[sqlmodel(computed)]` ([`847eca9`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/847eca9696c3e7e1dce646828faf68acef875a25), [`2cf3f57`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/2cf3f578b0f391a0f4edc8b37275f73398805c42)).
- Table inheritance strategy support (STI and joined) ([`cd07279`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/cd0727944bfc192b493b48ad6dd2a82b8f400cc0)).
- Relationship attribute parsing in proc macro ([`655fd58`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/655fd5871aed80bffccc456f0ebf6499bb77900e)).
- `ModelConfig` for Pydantic-style configuration ([`9ea7138`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/9ea71380a38b6b4873d1705af9ad92fd17bc1d92), [`7277b11`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/7277b11db8a75756ccf2ad962c52ea1edbc5c732)).
- `ModelEvents` trait for instance lifecycle hooks ([`a6ddb5b`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/a6ddb5b882c354c39aab6527038786bcd9da256f)).
- Field alias system for serialization ([`ba2ffda`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/ba2ffda177b330a66d6ee7d7ac464f87a065d8d8)).

### Value type system

- Automatic Rust-to-SQL type mapping for i8 through i64, f32/f64, bool, String, `Vec<u8>`, chrono, uuid.
- `FromValue` implementations for unsigned integer types ([`b5b8b64`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/b5b8b64a1c9d7161ce70a831b2d6fc4e60056f07)).
- `From`/`TryFrom` implementations for `Vec<T>` on `Value` type ([`ad98aba`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/ad98abaacbbc59106318236cdeebab81895f9c5e)).
- Precision loss detection for f32/f64 conversions ([`1a31f82`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/1a31f82e3f468d38a27f0ab31d4d590d38c0ab2b)).
- Silent u64-to-i64 overflow prevention ([`a441c90`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/a441c90d93c64c313025945ac54aa3b28f8b5434)).
- PostgreSQL ARRAY type support ([`755000d`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/755000d97b161be4fe0c06c1dbd8ede0dec0b6dd)).
- Bool handling added to `TryFrom<Value>` for f32/f64 ([`d9d53d5`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/d9d53d518ab2e1f59dee91192be72d1183c6a21e)).

### Dynamic and computed features

- Dynamic model creation at runtime ([`a1f9853`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/a1f9853c101d006c6728556b14ddecb90f89aed3)).
- Hybrid properties (computed fields that combine stored and derived values) ([`4aecd67`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/4aecd67980f359a734c442c9c84dae083918e936)).
- Attribute change events for tracking field mutations ([`ceaef88`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/ceaef881e736042c99b9d2e2a9db9bb8ad585230)).
- Runtime validation module with regex caching ([`b701950`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/b701950868b2a233adb07e5ea2549c441ab2c98b)).

### Model serialization

- `model_validate()` with Pydantic-compatible options ([`020bffb`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/020bffb260243fe5e943d152a8773a583cd0e1a7)).
- `model_dump()` with all Pydantic options (include, exclude, by_alias, exclude_unset, etc.) ([`5a19d1d`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/5a19d1db5cf6a383c6c471dd2d755383506d58b2), [`f99112b`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/f99112b38e6978f3e79233f0a9db4e16be5aabb8)).
- Alias-aware validation and serialization system ([`73ace45`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/73ace45519e1aa2cc4fec121b744fbcdd9327e6a)).

### Query builder (sqlmodel-query)

- Fluent type-safe query DSL for SELECT, INSERT, UPDATE, DELETE ([`28b9421`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/28b9421c1a6e6849eeb04f436a702a9495f3516f)).
- Multi-dialect SQL generation: Postgres, SQLite, MySQL ([`b2556ca`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/b2556caec25b347a2334ddad600916aadf84c427)).
- Expression builder with comparisons, NULL checks, LIKE/ILIKE, IN, BETWEEN, aggregates, CASE expressions.
- NULL-safe comparisons, CAST, and string pattern helpers ([`b7d8ea6`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/b7d8ea6491fdfdca46cddd406b3f6a00459cb3c1)).
- Dialect-aware identifier quoting throughout ([`635a248`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/635a2481e35cd7f93d5fed47d1275658b67859dd)).
- Window functions with `OVER` clause ([`ff276e0`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/ff276e08fcfac9bb1d9a5847877999071d3b9fad)).
- Common Table Expressions (CTEs) ([`ec9b666`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/ec9b6668de7ce90526dc6094f020118fe6cb4264)).
- Set operations: UNION, INTERSECT, EXCEPT ([`9aecd52`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/9aecd52644e3a975db00c87c28f20dd31775957d)).
- LATERAL subquery support ([`c4cdafa`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/c4cdafaeda1f14c7d4408ab4c65e4b669da42408)).
- Statement caching with LRU eviction ([`d12b98a`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/d12b98ac33f2311a19007ef34ce173cce6e41ae1)).
- MySQL CONCAT operator compatibility ([`e1822e9`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/e1822e99513ea162d66c1f2791d5251b939390f9)).
- DDL/DML identifier quoting integration tests ([`54dd4ca`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/54dd4ca59cada0855fa7587cd90d17d7fce1eb5c)).

### Schema and migrations (sqlmodel-schema)

- DDL generation from `Model` definitions ([`a16984f`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/a16984f9e2a5d326aaf61a97ff3d494b3aa2f17d)).
- Schema extraction from Model definitions ([`b64bfd2`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/b64bfd29891c883d98bf6e27c29f1e317ef96c33)).
- Schema diff engine for automatic migration generation ([`8f5ff2b`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/8f5ff2be96802671e759c118f199d7358b4a7452)).
- Migration file generator ([`16064f4`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/16064f436378409938200f533e764b544581351e)).
- `sql_type` attribute wired into DDL override ([`6bfc59a`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/6bfc59ac78409791f74a90847ffa7957ccda749e), [`4ba14c8`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/4ba14c8b104a6a530285888a31aaa4a44de2e79a)).
- Schema introspection for all three database types ([`3b83df7`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/3b83df7fc8a635cea314197d7af3a548525f40d2)).
- Shared `quote_ident` / `sanitize_identifier` across schema and session ([`a7da01d`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/a7da01d97a54c3e296a9395a0b22f6cea533a907)).
- SQL identifiers properly escaped in CREATE TABLE generation ([`b60313f`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/b60313fb5d6f5f1498eccde14df8f68d05b15381)).

### Session and unit of work (sqlmodel-session)

- `Session` with identity map and unit-of-work pattern ([`c511768`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/c5117682e8637f7fc8156b3e232d5aedb3c27395)).
- Identity Map pattern implementation ([`0e838dc`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/0e838dc72fcfd6b64388991074b3d94b3a507406)).
- Unit of Work pattern with dependency ordering ([`80eba4a`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/80eba4a11c154451acdae40d00ffded30d652d52)).
- `Session.get()` with composite PK and `FOR UPDATE` locking ([`d326695`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/d326695933b7c8a7fd6a4f9f9ce82dfe5d03e984)).
- `Session.merge()` for reattaching detached objects ([`6034ad3`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/6034ad38edf9a77df0b1983ad0cc874d27fc3d5d)).
- Change tracking and dirty detection with `is_modified()` and `modified_attributes()` ([`61b4a11`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/61b4a1182fec6f556d2f163c782af07e96adc2de), [`da25a50`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/da25a50a27b3c3dea5ae3f12329133e01cc454e2)).
- `Session::flush()` with operation ordering, batching, un-delete fix, and defensive state checks ([`030dfd4`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/030dfd47d20d6ddfd7493e2a3c88c7b7bc32189f), [`0a22269`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/0a222699fb6e0c84ac420c61eb21226272958c00), [`1127ae2`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/1127ae2a47af9978cd461923a711bed3940904b3)).
- Many-to-many relationship flush with `LinkTableOp` ([`3aa316a`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/3aa316aad0bab74e5d34dabab523dc0e6403f378), [`4b7c242`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/4b7c242288075d458e84d1472e87268d9452769c)).
- `Lazy<T>` wrapper type for deferred loading with `Session::load_lazy` / `load_many` ([`3ba0948`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/3ba0948fc63194a52babf46a4f058fed86a8aa1b), [`1d28c1a`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/1d28c1aa6814964fac61ce65d6c0554855a9366b)).
- Eager loading infrastructure with hydration and tracing ([`2a97f40`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/2a97f409e15d52c8049e2a8e1e68e5cef545789d), [`c65ef18`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/c65ef18f7b71af144f5e84acaeacf9959ce5c752)).
- Bulk insert and bulk update operations ([`4b9fecd`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/4b9fecd70955f34600cfe0bfb615821a2196ccf5)).
- N+1 detection with `N1DetectionScope` RAII guard ([`afcf3aa`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/afcf3aa9054d6288863129bb178b958d535960a8), [`72c29e3`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/72c29e36744ef3465de18c9b78415ab08a83b7c7)).
- Session-level event hooks (before_flush, after_flush, etc.) ([`065f0ff`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/065f0ffd3feada361c456b8097f51cc61101ee2c)).
- DECIMAL precision/scale support ([`72c29e3`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/72c29e36744ef3465de18c9b78415ab08a83b7c7)).
- Relationship metadata types and `Related<T>` wrapper ([`0f60318`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/0f603184310010c2c3f368a7e7f6a7af0321fc9e), [`aaf5ceb`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/aaf5ceb670ed4b3e58970ea29a0b6135378ff027)).
- Relationship lookup and validation helpers ([`d063050`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/d0630501b2aa6b41967abf6f43518c1a3a5a41d9)).

### Connection pooling (sqlmodel-pool)

- Generic async connection pool with timeouts, health checks, and metrics.
- Poisoned mutex handling: replaced `expect()` panics with proper error propagation ([`054f9f6`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/054f9f6bb5b7602df6c6baa0e265f96dc62632f5)).
- `PoolErrorKind::Poisoned` variant for mutex poison scenarios ([`31d1603`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/31d160364bbdf3b0b7632e459a9484cc7f9e595a)).
- Read replica routing ([`3246849`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/324684972e48a5075f0f26e4a8437930c4be43d5)).
- Horizontal sharding infrastructure ([`faf4792`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/faf47928bbbc3808ccc7e14277205bd2cc769615)).

### PostgreSQL driver (sqlmodel-postgres)

- Full wire protocol implementation with SCRAM-SHA-256 authentication ([`f5d8b14`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/f5d8b140e0b9a4db043f4b25c268d7ac71ba8526)).
- SCRAM authentication hardened against timing attacks ([`80e37df`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/80e37df1a5590581f94c5fff82d2ff7c62bb9c78)).
- PostgreSQL type system and OID mapping ([`4cceb26`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/4cceb26fc1df58bb2c241740143033600c554c76)).
- Async connection with `Connection` trait ([`29570fc`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/29570fc0d27cc37912f5c4076ffe0c89d0e39a56)).
- Console integration for connection progress display ([`89416cc`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/89416cca2c5f103128fd7ca556b669ead8ce15df)).

### MySQL driver (sqlmodel-mysql)

- Sync query methods and parameter binding ([`9a874e4`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/9a874e4dec1b5bb3e262179fc3dd0823b9102ad6)).
- Async `MySqlConnection` with `Connection` trait and `TransactionOps` ([`de7cbe3`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/de7cbe3b3ce11d2f514d78bb6e134c06a6ca9a80)).
- TLS/SSL configuration and full TLS support using rustls ([`6886daf`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/6886daf35bdfdd0a209ac4b3568d866975b50679), [`9b2c4ae`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/9b2c4ae0f2fbcde0d0463867dbee5d8866c82869)).
- Prepared statement binary protocol with integration ([`cb559cd`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/cb559cd1558de5f20bbbdcebb5bd548ad77dbf4f), [`f19f04e`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/f19f04e5d9cc7d7ec97538d54d2556408f9c3446)).
- TCP fragmentation handling in async packet I/O ([`a796bf3`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/a796bf38bdc91cf42a94a276c74761369e88d404)).
- Savepoint name validation to prevent SQL injection ([`a08940a`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/a08940a580eecce3660e68d3c7e69f6ab6162b79)).

### SQLite driver (sqlmodel-sqlite)

- FFI-based SQLite driver with `Connection` trait implementation ([`0c38f45`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/0c38f455b26f30d87ddfaf4abc474f4e38cb226e)).
- Rust 2024 edition compatibility fixes ([`fc5ba1e`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/fc5ba1e65163b26709a1da261b7f28b2ceaf9bcb)).
- Enhanced connection handling ([`d6622bd`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/d6622bd344cae4e9a2530241476865dbe41bfb03)).

### Console output (sqlmodel-console)

- Rich terminal output system with styled errors, query result tables, schema trees, and progress bars ([`47ff74a`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/47ff74a20f6d8b445361634674be818cfe973d42)).
- Agent auto-detection: automatically switches to plain text when running under Claude Code, Codex CLI, Cursor, Aider, etc. ([`0ec98ea`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/0ec98eaf1cfd4a95fdfd530ee38c70b5aa0e998e)).
- Three output modes: Rich (human TTY), Plain (agent/piped), JSON (`SQLMODEL_JSON=1`).
- Renderables: `ErrorPanel`, `PoolStatusDisplay`, `QueryResultTable`, `SchemaTree`, `MigrationStatus`, `TableInfo`, `OperationProgress`, `IndeterminateSpinner`, `BatchOperationTracker`.
- DDL syntax highlighting for schema output ([`e4925d4`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/e4925d458de888a35cee523bd997e4d492839050)).
- Migration status and table info renderables ([`183a1fb`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/183a1fbb90cff4b4e0c136fe771991a08cbc63e9)).
- E2E test suite and benchmarks ([`adad4e1`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/adad4e1faf8b3dc223dec4f74a49d66382a07d6f), [`598933a`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/598933a953a6aa481e7da0a2bdc3d6850e0348a9)).

### Facade crate (sqlmodel)

- Re-exports all workspace crates for single-import usage via `use sqlmodel::prelude::*`.
- Console re-exports in prelude; global console support for convenience.
- `SessionBuilder` with console integration ([`7894a82`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/7894a82f8ab19b09b7e72bb4302d8b6a189c5f33)).

### CI and infrastructure

- GitHub Actions CI/CD workflows for build, test, clippy, and fmt ([`04a8044`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/04a8044d60831cffae4a095cb9c4f8e57897ab61)).
- Extensive CI stabilization: flaky environment-variable-dependent agent-detection tests isolated and marked `#[ignore]`.
- Architecture-portable hash and improved change tracking diagnostics ([`4fd6648`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/4fd66488bf45b0d2c6b5ec64baffeb9e581cf2a0)).
- Major functionality completion milestone: drivers, pooling, transactions, and migrations all operational ([`c512642`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/c512642efbc00dfdc3e7bef59040318c2ecf6d5d)).

### Documentation

- Proposed Rust Architecture document ([`9a64a06`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/9a64a061dd3317f5828938dc4857a7d698ffc315)).
- Existing SQLModel Structure (behavior specification from Python) ([`bbb43f1`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/bbb43f1450a8949a40666164980c0af8a7ac9666)).
- Feature Parity Tracker (`FEATURE_PARITY.md`) reaching 92%+ coverage ([`493b437`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/493b437a4a7827760de5006c586ffd1bf60c61d6)).
- Console User Guide and Agent Compatibility Guide ([`5cbd35e`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/5cbd35ec185d9cbed2cf270a4219535807a9febc), [`680cace`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/680cace274fe0000178af628b8e178a5a6964170)).
- MIT License added ([`196f5da`](https://github.com/Dicklesworthstone/sqlmodel_rust/commit/196f5da935cdc79397c99044902b5914f16d0c10)).

---

[0.2.1]: https://github.com/Dicklesworthstone/sqlmodel_rust/compare/v0.2.0...main
[0.2.0]: https://github.com/Dicklesworthstone/sqlmodel_rust/releases/tag/v0.2.0
[0.1.1]: https://github.com/Dicklesworthstone/sqlmodel_rust/compare/37e6a0c...d14218b
