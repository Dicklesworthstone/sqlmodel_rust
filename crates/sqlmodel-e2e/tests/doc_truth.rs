//! Doc-truth guard (`bd-si4u.6`).
//!
//! Doc drift accumulated silently for seven months (a stale "Last verified"
//! date, a crate missing from README's architecture, a nonexistent `tests/`
//! directory cited by AGENTS.md). Humans will not re-audit monthly; this test
//! does. It reads the repository's markdown at test time and asserts:
//!
//! 1. every crate directory appears in README's Crate Responsibilities table
//!    and in AGENTS.md;
//! 2. every crate ships a `README.md` and points at it from `Cargo.toml`;
//! 3. every `src/<file>` referenced by AGENTS.md's "Key Files by Crate"
//!    table exists;
//! 4. every bead ID cited in the planning documents exists in
//!    `.beads/issues.jsonl` (historical footprints on lines that explicitly
//!    say the bead "no longer exists" are exempt; a missing tracker file fails
//!    unless `SQLMODEL_ALLOW_MISSING_BEADS_TRACKER=1` is set intentionally for
//!    stripped remote-worker closures, in which case the gate must count the
//!    check as not run);
//! 5. the README install snippet versions equal the workspace version;
//! 6. FEATURE_PARITY's last-verified date is not older than the newest
//!    dated CHANGELOG release entry (a release re-verifies parity);
//! 7. no markdown contains a retired phrase (each entry documents why).
//!
//! Failures print the file, the line number, and the offending text.
//! Pure filesystem reads — runs in well under a second, no network.

use std::fs;
use std::path::{Path, PathBuf};

/// Repository root, derived from this crate's manifest directory.
///
/// Prefers the runtime `CARGO_MANIFEST_DIR` provided by `cargo test` so a shared
/// `CARGO_TARGET_DIR` across checkouts always inspects the tree under test,
/// falling back to the compile-time path when the test binary is run directly.
fn repo_root() -> PathBuf {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")), PathBuf::from);
    manifest_dir
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root resolves")
}

fn read(rel: &str) -> String {
    fs::read_to_string(repo_root().join(rel))
        .unwrap_or_else(|error| panic!("cannot read {rel}: {error}"))
}

fn crate_directories() -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(repo_root().join("crates"))
        .expect("crates/ directory")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect();
    names.sort();
    names
}

/// The slice of a markdown document between a `### heading` and the next
/// same-or-higher heading.
fn section<'a>(content: &'a str, heading: &str) -> &'a str {
    let marker = format!("### {heading}");
    let start = content
        .find(&marker)
        .unwrap_or_else(|| panic!("document lacks a `{marker}` section"));
    let after = &content[start + marker.len()..];
    let end = after
        .find("\n### ")
        .or_else(|| after.find("\n## "))
        .map_or(content.len(), |offset| start + marker.len() + offset);
    &content[start..end]
}

#[test]
fn every_crate_is_documented_and_ships_a_readme() {
    let readme = read("README.md");
    let agents = read("AGENTS.md");
    let responsibilities = section(&readme, "Crate Responsibilities");

    for name in crate_directories() {
        assert!(
            responsibilities.contains(&format!("`{name}`")),
            "README's Crate Responsibilities table does not list `{name}`; table slice:\n{responsibilities}"
        );
        assert!(
            agents.contains(&name),
            "AGENTS.md never mentions the crate `{name}`"
        );
        let readme_path = repo_root().join("crates").join(&name).join("README.md");
        assert!(
            readme_path.is_file(),
            "crates/{name}/README.md is missing (every crate ships one)"
        );
        let manifest =
            fs::read_to_string(repo_root().join("crates").join(&name).join("Cargo.toml"))
                .expect("crate manifest");
        assert!(
            manifest.contains("readme = \"README.md\""),
            "crates/{name}/Cargo.toml does not set readme = \"README.md\""
        );
    }
}

#[test]
fn agents_key_files_reference_existing_paths() {
    let agents = read("AGENTS.md");
    let key_files = section(&agents, "Key Files by Crate");
    for line in key_files.lines() {
        // Table rows look like: | `sqlmodel-core` | `src/lib.rs` | purpose |
        let Some(rest) = line.trim().strip_prefix("| `sqlmodel-") else {
            continue;
        };
        let Some(crate_end) = rest.find('`') else {
            continue;
        };
        let crate_name = format!("sqlmodel-{}", &rest[..crate_end]);
        let after_crate = &rest[crate_end + 1..];
        let Some(src_start) = after_crate.find("`src/") else {
            continue;
        };
        let path_part = &after_crate[src_start + 1..];
        let Some(path_end) = path_part.find('`') else {
            continue;
        };
        let src_path = &path_part[..path_end];
        let full = repo_root().join("crates").join(&crate_name).join(src_path);
        assert!(
            full.is_file(),
            "AGENTS.md Key Files by Crate cites {crate_name}/{src_path}, which does not exist"
        );
    }
}

const ALLOW_MISSING_BEADS_TRACKER_ENV: &str = "SQLMODEL_ALLOW_MISSING_BEADS_TRACKER";

#[derive(Debug, PartialEq, Eq)]
enum BeadsTrackerLoad {
    Loaded(String),
    OptedOutMissing { path: PathBuf },
}

fn env_flag_enabled(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn load_beads_tracker(root: &Path, allow_missing: bool) -> Result<BeadsTrackerLoad, String> {
    load_beads_tracker_at(&root.join(".beads/issues.jsonl"), allow_missing)
}

fn load_beads_tracker_at(
    tracker_path: &Path,
    allow_missing: bool,
) -> Result<BeadsTrackerLoad, String> {
    match fs::read_to_string(tracker_path) {
        Ok(contents) => Ok(BeadsTrackerLoad::Loaded(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if allow_missing {
                Ok(BeadsTrackerLoad::OptedOutMissing {
                    path: tracker_path.to_path_buf(),
                })
            } else {
                Err(format!(
                    "missing beads issue tracker at {} ({error}): `.beads/issues.jsonl` is checked \
                     into the repository, so its absence indicates an incomplete tree or stripped \
                     remote-worker closure; run on a full checkout, or set \
                     {ALLOW_MISSING_BEADS_TRACKER_ENV}=1 intentionally (and record \
                     `cited_bead_ids_exist_in_the_tracker` as NOT RUN)",
                    tracker_path.display()
                ))
            }
        }
        Err(error) => Err(format!(
            "cannot read beads issue tracker at {}: {error}",
            tracker_path.display()
        )),
    }
}

#[test]
fn cited_bead_ids_exist_in_the_tracker() {
    let documents = [
        "README.md",
        "AGENTS.md",
        "PLAN_TO_PORT_SQLMODEL_TO_RUST.md",
        "PROPOSED_RUST_ARCHITECTURE.md",
        "FEATURE_PARITY.md",
        "EXISTING_SQLMODEL_STRUCTURE.md",
    ];
    let allow_missing = env_flag_enabled(
        std::env::var(ALLOW_MISSING_BEADS_TRACKER_ENV)
            .ok()
            .as_deref(),
    );
    let tracker = match load_beads_tracker(&repo_root(), allow_missing) {
        Ok(BeadsTrackerLoad::Loaded(contents)) => contents,
        Ok(BeadsTrackerLoad::OptedOutMissing { path }) => {
            eprintln!(
                "NOT RUN: cited_bead_ids_exist_in_the_tracker opted out via \
                 {ALLOW_MISSING_BEADS_TRACKER_ENV}=1 because {} is absent \
                 (gates setting this opt-out must record this check as not run)",
                path.display()
            );
            return;
        }
        Err(message) => panic!("{message}"),
    };
    let mut unknown: Vec<(String, String)> = Vec::new();
    for document in documents {
        let content = read(document);
        for (index, line) in content.lines().enumerate() {
            // Historical footprints explicitly flag themselves; a bead cited
            // as "no longer exists" is documentation, not drift.
            if line.contains("no longer exists") {
                continue;
            }
            for id in bead_ids_in(line) {
                let present = tracker
                    .lines()
                    .any(|entry| entry.contains(&format!("\"{id}\"")));
                if !present {
                    unknown.push((format!("{document}:{}", index + 1), id));
                }
            }
        }
    }
    assert!(
        unknown.is_empty(),
        "documents cite bead IDs that do not exist in .beads/issues.jsonl: {unknown:?}"
    );
}

/// Extracts `bd-<id>` tokens (optionally dotted, like `bd-x6jl.2`) from a
/// line without a regex dependency.
fn bead_ids_in(line: &str) -> Vec<String> {
    let bytes = line.as_bytes();
    let mut ids = Vec::new();
    let mut index = 0;
    while let Some(start) = line[index..].find("bd-") {
        let start = index + start;
        let mut end = start + 3;
        while end < bytes.len() {
            let b = bytes[end];
            if b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' {
                end += 1;
            } else {
                break;
            }
        }
        // Trim trailing dot (sentence punctuation is not part of the id).
        let mut token = &line[start..end];
        while token.ends_with('.') {
            token = &token[..token.len() - 1];
        }
        if token.len() > 3 {
            ids.push(token.to_owned());
        }
        index = end;
    }
    ids.sort();
    ids.dedup();
    ids
}

#[test]
fn readme_versions_match_the_workspace_version() {
    let manifest = read("Cargo.toml");
    let package_section = manifest
        .split("[workspace.package]")
        .nth(1)
        .expect("root Cargo.toml has [workspace.package]");
    let version = package_section
        .lines()
        .find_map(|line| {
            let value = line.trim().strip_prefix("version = \"")?;
            value.strip_suffix('"')
        })
        .expect("workspace version");
    assert!(!version.is_empty(), "workspace version is set");

    let readme = read("README.md");
    let mut offenders: Vec<String> = Vec::new();
    for (index, line) in readme.lines().enumerate() {
        if !(line.contains("sqlmodel = {")
            || line.contains("sqlmodel-")
            || line.contains("sqlmodel = \""))
        {
            continue;
        }
        for cited in cited_sqlmodel_versions(line) {
            // `0.4` is compatible with workspace `0.4.2`: a cited version is
            // fine when the workspace version extends it at a dot boundary.
            let compatible = cited == version || version.starts_with(&format!("{cited}."));
            if !compatible {
                offenders.push(format!(
                    "line {}: {line} (cited {cited}, workspace {version})",
                    index + 1
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "README cites a version that is not the workspace version {version}: {offenders:?}"
    );
}

/// Every `version = "..."` value on the line, in order.
fn cited_sqlmodel_versions(line: &str) -> Vec<&str> {
    let mut versions = Vec::new();
    let mut rest = line;
    while let Some(position) = rest.find("version = \"") {
        let after = &rest[position + "version = \"".len()..];
        match after.find('"') {
            Some(end) => {
                versions.push(&after[..end]);
                rest = &after[end..];
            }
            None => break,
        }
    }
    versions
}

#[test]
fn feature_parity_is_verified_against_the_newest_release() {
    let changelog = read("CHANGELOG.md");
    let newest_release = changelog
        .lines()
        .find_map(|line| {
            let entry = line.trim().strip_prefix("## [")?;
            if entry.starts_with("Unreleased") {
                return None;
            }
            // `## [0.4.2] -- 2026-08-31` -> "2026-08-31"
            find_date(entry)
        })
        .expect("CHANGELOG has a dated release entry");
    let parity = read("FEATURE_PARITY.md");
    let verified_line = parity
        .lines()
        .find(|line| line.contains("Last Updated"))
        .expect("FEATURE_PARITY carries a Last Updated line");
    let verified = find_date(verified_line)
        .unwrap_or_else(|| panic!("no YYYY-MM-DD date found in: {verified_line}"));
    assert!(
        newest_release.as_str() <= verified.as_str(),
        "FEATURE_PARITY was last verified {verified}, but CHANGELOG's newest release is {newest_release}; \
         a release must re-verify parity"
    );
}

/// Pulls the first `YYYY-MM-DD` looking token out of a line.
fn find_date(line: &str) -> Option<String> {
    let bytes = line.as_bytes();
    for start in 0..bytes.len() {
        if start + 10 <= bytes.len()
            && bytes[start..start + 10]
                .iter()
                .enumerate()
                .all(|(offset, b)| {
                    if offset == 4 || offset == 7 {
                        *b == b'-'
                    } else {
                        b.is_ascii_digit()
                    }
                })
        {
            return Some(line[start..start + 10].to_owned());
        }
    }
    None
}

#[test]
fn no_markdown_contains_retired_phrases() {
    // Each banned phrase documents why it was retired.
    const RETIRED: [(&str, &str); 4] = [
        (
            "edition 2024 is unstable",
            "the workspace builds on stable-elected Rust 2024; the phrase only ever described a long-gone bootstrap state",
        ),
        (
            "TCP/IO | 🔜",
            "stale planning-table cell; the TCP/IO story shipped and is documented in prose instead",
        ),
        (
            "raw_query!",
            "the macro was removed from the API surface; raw SQL goes through the connection execute methods",
        ),
        (
            "Limited documentation",
            "the 11-chapter ORM user guide in docs/guide/ provides comprehensive, doctest-compiled documentation",
        ),
    ];
    let mut documents: Vec<PathBuf> = fs::read_dir(repo_root())
        .expect("repo root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .collect();
    for entry in fs::read_dir(repo_root().join("crates")).expect("crates/") {
        let readme = entry.expect("crate entry").path().join("README.md");
        if readme.is_file() {
            documents.push(readme);
        }
    }

    let mut offenders: Vec<String> = Vec::new();
    for document in &documents {
        let content = fs::read_to_string(document)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", document.display()));
        for (phrase, reason) in RETIRED {
            for (index, line) in content.lines().enumerate() {
                if line.contains(phrase) {
                    offenders.push(format!(
                        "{}:{} contains retired phrase {phrase:?} ({reason}): {line}",
                        document.display(),
                        index + 1
                    ));
                }
            }
        }
    }
    assert!(offenders.is_empty(), "retired phrases found: {offenders:?}");
}

#[test]
fn guide_docs_are_synchronized() {
    let root_guide = repo_root().join("docs").join("guide");
    let crate_guide = repo_root()
        .join("crates")
        .join("sqlmodel")
        .join("docs")
        .join("guide");

    let entries = fs::read_dir(&root_guide).expect("read docs/guide");
    for entry in entries {
        let entry = entry.expect("entry");
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "md") {
            let file_name = path.file_name().expect("file name");
            let crate_path = crate_guide.join(file_name);
            assert!(
                crate_path.is_file(),
                "missing packaged guide file: {}",
                crate_path.display()
            );
            let root_content = fs::read(&path).expect("read root guide");
            let crate_content = fs::read(&crate_path).expect("read crate guide");
            assert_eq!(
                root_content,
                crate_content,
                "guide doc drift between {} and {}",
                path.display(),
                crate_path.display()
            );
        }
    }
}

#[test]
fn missing_beads_tracker_fails_without_explicit_opt_out() {
    let dir_without_beads = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let err = load_beads_tracker(&dir_without_beads, false)
        .expect_err("missing .beads/issues.jsonl must fail by default");
    assert!(
        err.contains(".beads/issues.jsonl"),
        "diagnostic must name missing tracker path: {err}"
    );
    assert!(
        err.contains(ALLOW_MISSING_BEADS_TRACKER_ENV),
        "diagnostic must name explicit opt-out variable: {err}"
    );
}

#[test]
fn missing_beads_tracker_opt_out_is_explicit_and_does_not_mask_io_errors() {
    let dir_without_beads = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let opted_out = load_beads_tracker(&dir_without_beads, true)
        .expect("explicit opt-out allows missing .beads/issues.jsonl");
    assert_eq!(
        opted_out,
        BeadsTrackerLoad::OptedOutMissing {
            path: dir_without_beads.join(".beads/issues.jsonl"),
        }
    );

    // Existing readable file loads normally regardless of opt-out.
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let loaded = load_beads_tracker_at(&manifest, false).expect("existing file loads");
    assert!(matches!(loaded, BeadsTrackerLoad::Loaded(s) if s.contains("sqlmodel-e2e")));

    // Non-NotFound errors (such as pointing at a directory) still fail even with opt-out.
    let dir_err = load_beads_tracker_at(&dir_without_beads, true)
        .expect_err("non-NotFound IO errors must not be masked by opt-out");
    assert!(
        dir_err.contains("cannot read beads issue tracker"),
        "unexpected error: {dir_err}"
    );

    for disabled in [None, Some(""), Some("0"), Some("false"), Some("no")] {
        assert!(
            !env_flag_enabled(disabled),
            "should be disabled: {disabled:?}"
        );
    }
    for enabled in [
        Some("1"),
        Some(" 1 "),
        Some("true"),
        Some("TRUE"),
        Some("yes"),
    ] {
        assert!(env_flag_enabled(enabled), "should be enabled: {enabled:?}");
    }
}
