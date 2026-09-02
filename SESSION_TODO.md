# Session TODO (retired)

**Status:** retired on 2026-09-02. This file was a Codex session scratchpad from 2026-02-10 that
tracked parity work item by item. The live source of truth for remaining work is the Beads tracker
(`br ready`, `bv --robot-triage`). Nothing below is actionable; it is kept only because this
repository never deletes files without explicit permission (AGENTS.md rule 1), and so the unchecked
items it carried have a recorded disposition.

## Disposition of every item that was still unchecked

| Former item | Disposition |
|---|---|
| Joined inheritance: extend polymorphic support beyond a single child type | Done for 2 and 3 children (`fb379fe`, `polymorphic_joined2/3`); N-ary generation is bd-kzp1.3 |
| Joined inheritance: track/decide DML semantics across base+child | Done (`8784476`, `d9ed0a9`); auto-increment upsert and RETURNING with ON CONFLICT remain as bd-kzp1.4 |
| bd-3bmd block: explicit UPDATE SET/WHERE, DELETE WHERE (+RETURNING), INSERT ON CONFLICT for joined children | Implemented in `d9ed0a9` (`crates/sqlmodel-query/src/builder.rs`); the end-to-end coverage lives in `crates/sqlmodel/tests/joined_inheritance_dml_sqlite.rs` rather than the separately planned `..._advanced_sqlite.rs` file. Auto-increment upsert and `insert_returning` with ON CONFLICT remain unsupported by design and are bd-kzp1.4 |
| bd-3bmd quality gates | Superseded by the workspace gates run in CI |
| B2: remove obsolete "Rust Equivalent (Serde only)" guidance in EXISTING_SQLMODEL_STRUCTURE.md; never claim unverified features | bd-si4u.1 (doc-truth pass) |
| B4 / C: doc quality gates, landing the plane | Session ended; no action |
| E2: parse MySQL text-protocol temporal strings into structured `Value` | Done (`952051b`, bd-22u8) |
| E3: reconcile "Explicitly Excluded" content with the no-exclusions policy | bd-si4u.1 rewrites EXISTING_SQLMODEL_STRUCTURE §12 as design differences |
| H5: joined polymorphism API / hydration / bead | Done (`f70720e`); remaining inheritance work is bd-kzp1.1 through bd-kzp1.7 |
| F3: audit README/FEATURE_PARITY for the old `Session::builder` meaning | Verified 2026-09-02: no such references remain |
| F3: decide identity-map reference identity vs value caching | Decided and implemented: `Arc<RwLock<M>>` reference identity in `crates/sqlmodel-session/src/identity_map.rs` |

## Bead IDs this file used to cite

bd-ukkg, bd-2bht, bd-4bhg, bd-3bmd, bd-3j44, bd-3g6y, bd-ywnj, bd-2lpn, bd-22u8, bd-1ytr, bd-3obp no
longer exist in `.beads/issues.jsonl`. Their commit mapping is recorded as comments on bd-kzp1 and
bd-162 (2026-09-02).
