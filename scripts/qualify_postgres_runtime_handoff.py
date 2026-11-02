#!/usr/bin/env python3
"""Qualify issue #43 with production features and independently bounded cases.

Builds are deliberately outside the per-case deadline. Each executed case must
exit successfully AND print its completion marker. Missing services, build
failures, timeouts, reconnects, or skipped cases never count as passes. Evidence
(manifest, lock, resolved features, logs, and summary) is retained for inspection.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
from urllib.parse import unquote, urlsplit


CASES = (
    "current-thread-normal",
    "current-thread-panic",
    "two-worker-normal",
    "two-worker-panic",
    "stale-socket-current-thread",
    "stale-socket-two-worker",
)
SQLMODEL_CRATES = ("sqlmodel-core", "sqlmodel-pool", "sqlmodel-postgres")


def postgres_environment(cases: list[str]) -> dict[str, str]:
    env = os.environ.copy()
    if all(case.startswith("stale-socket-") for case in cases):
        return env
    url = env.get("SQLMODEL_TEST_POSTGRES_URL")
    if not url:
        raise ValueError("SQLMODEL_TEST_POSTGRES_URL is required; PostgreSQL cases cannot skip")
    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme not in ("postgres", "postgresql")
            or not parsed.hostname
            or not parsed.username
            or not parsed.path.strip("/")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError
        env.update(
            SQLMODEL_HANDOFF_PG_HOST=parsed.hostname,
            SQLMODEL_HANDOFF_PG_PORT=str(parsed.port or 5432),
            SQLMODEL_HANDOFF_PG_USER=unquote(parsed.username),
            SQLMODEL_HANDOFF_PG_DATABASE=unquote(parsed.path.strip("/")),
        )
        env.pop("SQLMODEL_HANDOFF_PG_PASSWORD", None)
        if parsed.password is not None:
            env["SQLMODEL_HANDOFF_PG_PASSWORD"] = unquote(parsed.password)
    except ValueError:
        # Do not include a credential-bearing URL in diagnostics or evidence.
        raise ValueError(
            "SQLMODEL_TEST_POSTGRES_URL must be postgres://user[:password]@host[:port]/database "
            "without query parameters; this default-feature witness uses non-TLS PostgreSQL"
        ) from None
    return env


def workspace_runtime_version(root: Path) -> str:
    lock = tomllib.loads((root / "Cargo.lock").read_text())
    versions = {package["version"] for package in lock["package"] if package["name"] == "asupersync"}
    selected = set()
    for name in SQLMODEL_CRATES:
        packages = [p for p in lock["package"] if p["name"] == name and "source" not in p]
        if len(packages) != 1:
            raise ValueError(f"workspace lock must identify one local {name}")
        dependencies = [d.split() for d in packages[0]["dependencies"] if d.split()[0] == "asupersync"]
        if len(dependencies) != 1:
            raise ValueError(f"workspace lock must identify {name}'s direct Asupersync")
        dependency = dependencies[0]
        if len(dependency) > 1:
            selected.add(dependency[1])
        elif len(versions) == 1:
            selected.update(versions)
        else:
            raise ValueError(f"ambiguous Asupersync dependency for {name}")
    if len(selected) != 1 or not selected.issubset(versions):
        raise ValueError("the selected SQLModel crates must resolve the same Asupersync version")
    return selected.pop()


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stage_workspace(root: Path, evidence: Path, runtime_version: str) -> Path:
    """Copy the candidate source and change only its runtime requirement.

    This intentionally does not port the repository's all-driver graph. Its
    single-runtime release gate remains unchanged; the staged PostgreSQL-only
    graph provides integration evidence for the eventual coordinated port.
    """
    if evidence.resolve().is_relative_to((root / "crates").resolve()):
        raise ValueError("staged evidence must be outside the source crates directory")
    stage = evidence / "staged-workspace"
    stage.mkdir()
    shutil.copytree(root / "crates", stage / "crates", ignore=shutil.ignore_patterns("target"))
    for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "README.md", "LICENSE"):
        shutil.copyfile(root / name, stage / name)
    manifest = (stage / "Cargo.toml").read_text()
    dependency = tomllib.loads(manifest)["workspace"]["dependencies"]["asupersync"]
    old_line = f'asupersync = {{ version = "{dependency["version"]}", default-features = false }}'
    if manifest.splitlines().count(old_line) != 1:
        raise ValueError("runtime dependency format changed; review the staged override explicitly")
    new_line = f"asupersync = {{ version = {json.dumps(runtime_version)}, default-features = false }}"
    staged_manifest = manifest.replace(old_line, new_line, 1)
    (evidence / "original-workspace-Cargo.toml").write_text(manifest)
    (stage / "Cargo.toml").write_text(staged_manifest)
    return stage


def record_provenance(root: Path, source_root: Path, evidence: Path) -> None:
    files = sorted(p for p in (source_root / "crates").rglob("*") if p.is_file() and "target" not in p.parts)
    files.extend(source_root / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"))
    commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, text=True, capture_output=True, check=False)
    status = subprocess.run(["git", "status", "--porcelain"], cwd=root, text=True, capture_output=True, check=False)
    provenance = {
        "repository_commit": commit.stdout.strip() if commit.returncode == 0 else None,
        "repository_status": status.stdout.splitlines() if status.returncode == 0 else None,
        "staged": source_root != root,
        "original_manifest_sha256": file_hash(root / "Cargo.toml"),
        "fixture_sha256": file_hash(evidence / "main.rs"),
        "source_sha256": {str(p.relative_to(source_root)): file_hash(p) for p in files},
    }
    (evidence / "source-provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")


def fixture_manifest(args: argparse.Namespace, root: Path, runtime_version: str, source: Path) -> str:
    lines = [
        "[package]",
        'name = "sqlmodel-runtime-handoff-qualification"',
        'version = "0.0.0"',
        'edition = "2024"',
        "publish = false",
        "",
        "[workspace]",
        # Keep the copied crates in their own workspace so inherited package
        # fields and dependencies come from the staged workspace manifest.
        'exclude = ["staged-workspace"]',
        "",
        "[[bin]]",
        'name = "postgres-runtime-handoff"',
        f"path = {json.dumps(str(source))}",
        "",
        "[dependencies]",
        f"asupersync = {{ version = {json.dumps('=' + runtime_version)}, default-features = false }}",
    ]
    for name in SQLMODEL_CRATES:
        if args.source == "workspace":
            selection = f"path = {json.dumps(str(root / 'crates' / name))}"
        else:
            selection = f"version = {json.dumps('=' + args.sqlmodel_version)}"
        lines.append(f"{name} = {{ {selection}, default-features = false }}")
    if args.asupersync_path:
        lines.extend(
            (
                "",
                "[patch.crates-io]",
                f"asupersync = {{ path = {json.dumps(str(args.asupersync_path.resolve()))} }}",
            )
        )
    # Only debug builds are used: the intentional holder panic must unwind.
    lines.extend(("", "[profile.dev]", 'panic = "unwind"', ""))
    return "\n".join(lines)


def build_fixture(args: argparse.Namespace, root: Path, evidence: Path, runtime_version: str) -> Path:
    source_root = stage_workspace(root, evidence, runtime_version) if args.stage_asupersync_version else root
    source = evidence / "main.rs"
    shutil.copyfile(root / "crates/sqlmodel-e2e/runtime-handoff/main.rs", source)
    manifest = evidence / "Cargo.toml"
    manifest.write_text(fixture_manifest(args, source_root, runtime_version, source))
    # Match the project's toolchain even though this manifest lives outside it.
    (evidence / "rust-toolchain.toml").write_text((root / "rust-toolchain.toml").read_text())
    record_provenance(root, source_root, evidence)
    # Resolve once, inspect the actual production feature graph, then build
    # with --locked. Retain that lockfile with the qualification evidence.
    metadata = subprocess.check_output(
        [args.cargo, "metadata", "--format-version", "1", "--manifest-path", str(manifest)],
        cwd=root,
        text=True,
    )
    (evidence / "metadata.json").write_text(metadata)
    graph = json.loads(metadata)
    runtimes = [package for package in graph["packages"] if package["name"] == "asupersync"]
    if len(runtimes) != 1 or runtimes[0]["version"] != runtime_version:
        raise ValueError("qualification must resolve exactly the requested Asupersync version")
    runtime = runtimes[0]
    node = next(node for node in graph["resolve"]["nodes"] if node["id"] == runtime["id"])
    if "test-internals" in node["features"]:
        raise ValueError("production qualification must not enable Asupersync test-internals")
    if args.asupersync_path:
        expected_runtime = args.asupersync_path.resolve() / "Cargo.toml"
        if runtime["source"] is not None or Path(runtime["manifest_path"]).resolve() != expected_runtime:
            raise ValueError("path qualification did not resolve the selected Asupersync checkout")
    elif not (runtime["source"] or "").startswith("registry+"):
        raise ValueError("published-dependency qualification resolved a non-registry Asupersync")
    if args.source == "registry":
        for name in SQLMODEL_CRATES:
            packages = [package for package in graph["packages"] if package["name"] == name]
            if (
                len(packages) != 1
                or packages[0]["version"] != args.sqlmodel_version
                or not (packages[0]["source"] or "").startswith("registry+")
            ):
                raise ValueError(f"registry qualification did not resolve published {name}")
    else:
        for name in SQLMODEL_CRATES:
            packages = [package for package in graph["packages"] if package["name"] == name]
            expected = source_root / "crates" / name / "Cargo.toml"
            if len(packages) != 1 or Path(packages[0]["manifest_path"]).resolve() != expected.resolve():
                raise ValueError(f"qualification did not resolve the selected source for {name}")
    command = [
        args.cargo, "build", "--locked", "--manifest-path", str(manifest), "--message-format=json"
    ]
    with (evidence / "build.jsonl").open("w") as output, (evidence / "build.stderr.log").open("w") as errors:
        subprocess.run(command, cwd=root, stdout=output, stderr=errors, check=True)
    executable = None
    for line in (evidence / "build.jsonl").read_text().splitlines():
        message = json.loads(line)
        if message.get("reason") == "compiler-artifact" and message.get("executable"):
            executable = Path(message["executable"])
    if executable is None:
        raise ValueError("build produced no qualification executable")
    return executable


def run_case(executable: Path, case: str, timeout: float, env: dict[str, str], evidence: Path) -> dict:
    log = evidence / f"{case}.log"
    started = time.monotonic()
    with log.open("w") as output:
        child = subprocess.Popen([str(executable), case], env=env, stdout=output, stderr=subprocess.STDOUT)
        try:
            try:
                returncode = child.wait(timeout=timeout)
                status = "passed" if returncode == 0 else "failed"
            except subprocess.TimeoutExpired:
                # The witness creates threads, not subprocesses. Kill and reap
                # only this child, then continue to execute the control cases.
                child.kill()
                returncode = child.wait()
                status = "timed_out"
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
    if status == "passed" and f"PASS {case}" not in log.read_text().splitlines():
        status = "missing_completion_marker"
    elapsed = time.monotonic() - started
    print(f"{case}: {status} ({elapsed:.3f}s), log: {log}", flush=True)
    return {"case": case, "status": status, "returncode": returncode, "elapsed_seconds": elapsed}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("workspace", "registry"), default="workspace")
    parser.add_argument("--sqlmodel-version", help="exact published family version; required for registry")
    parser.add_argument("--asupersync-version", help="exact runtime version; defaults to workspace lock")
    parser.add_argument("--stage-asupersync-version", help="qualify a copied source tree with this runtime requirement")
    parser.add_argument("--asupersync-path", type=Path, help="explicit upstream checkout override (not release proof)")
    parser.add_argument("--case", choices=CASES, action="append", dest="cases", help="repeat to select cases")
    parser.add_argument("--timeout", type=float, default=60.0, help="seconds per executed case, excluding build")
    parser.add_argument("--evidence-dir", type=Path, help="new directory for manifest, lock, logs, and summary")
    parser.add_argument("--cargo", default="cargo", help="Cargo executable")
    args = parser.parse_args()
    if args.source == "registry" and (not args.sqlmodel_version or not args.asupersync_version):
        parser.error("registry mode requires --sqlmodel-version and --asupersync-version")
    if args.source == "workspace" and args.sqlmodel_version:
        parser.error("--sqlmodel-version applies only to registry mode")
    if args.source == "registry" and (args.stage_asupersync_version or args.asupersync_path):
        parser.error("registry qualification does not permit staged or path overrides")
    if args.stage_asupersync_version and args.asupersync_version:
        parser.error("use either --stage-asupersync-version or --asupersync-version")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be finite and positive")
    root = Path(__file__).resolve().parents[1]
    evidence_location = args.evidence_dir or Path(tempfile.gettempdir())
    if args.stage_asupersync_version and evidence_location.resolve().is_relative_to((root / "crates").resolve()):
        parser.error("staged evidence must be outside the source crates directory")
    cases = list(dict.fromkeys(args.cases or CASES))
    evidence = None
    summary = None
    try:
        env = postgres_environment(cases)
        runtime_version = args.stage_asupersync_version or args.asupersync_version or workspace_runtime_version(root)
        if args.evidence_dir:
            evidence = args.evidence_dir.resolve()
            evidence.mkdir(parents=True, exist_ok=False)
        else:
            evidence = Path(tempfile.mkdtemp(prefix="sqlmodel-runtime-handoff-"))
        print(f"Evidence: {evidence}", flush=True)
        summary = {
            "status": "building",
            "sqlmodel_source": "staged workspace" if args.stage_asupersync_version else args.source,
            "sqlmodel_version": args.sqlmodel_version,
            "asupersync_version": runtime_version,
            "asupersync_source": "path override" if args.asupersync_path else "published registry",
            "timeout_seconds": args.timeout,
            "expected_cases": cases,
            "cases": [],
        }
        summary_path = evidence / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2) + "\n")
        executable = build_fixture(args, root, evidence, runtime_version)
        summary["status"] = "running"
        for case in cases:
            summary["cases"].append(run_case(executable, case, args.timeout, env, evidence))
            summary_path.write_text(json.dumps(summary, indent=2) + "\n")
        failed = any(case["status"] != "passed" for case in summary["cases"])
        summary["status"] = "failed" if failed else "passed"
        summary_path.write_text(json.dumps(summary, indent=2) + "\n")
        return int(failed)
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"Qualification failed: {error}", file=sys.stderr)
        if evidence:
            if summary is not None:
                summary["status"] = "failed"
                (evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(f"Evidence retained: {evidence}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
