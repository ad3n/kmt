import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import tempfile
import time


FIXTURES = Path(__file__).resolve().parent / "testdata"
REPO = FIXTURES.parents[2]
WORKERS = (1, 2, 4)
TABLES = ",".join(f"table_{index:02d}" for index in range(8))
SEED_FILES = ("3000000000_seed.up.sql", "3000000000_seed.down.sql")
MIB = 1024 * 1024


def build_fixture(directory):
    main = REPO / "main.go"
    source = main.read_text(encoding="utf-8")
    start = re.search(r'^([ \t]+)Name:[ \t]+"generate",', source, re.MULTILINE)
    if start is None:
        raise ValueError("could not find the generate command in main.go")
    remaining = source[start.end():]
    next_command = re.search(r"^" + re.escape(start[1]) + r"Name:", remaining, re.MULTILINE)
    end = start.end() + next_command.start() if next_command else len(source)
    command = source[start.start():end]
    connection = "config.NewConnection(source)"
    if command.count(connection) != 1:
        raise ValueError("could not identify a single database connection in generate")
    command = command.replace(connection, "benchmarkConnection(source)")

    overlaid_main = directory / "main.go"
    overlaid_main.write_text(source[:start.start()] + command + source[end:], encoding="utf-8")
    overlay = directory / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {
        str(main): str(overlaid_main),
        str(REPO / "benchmark_generate_fixture.go"): str(FIXTURES / "connection.go"),
    }}), encoding="utf-8")

    binary = directory / "kmt"
    dump = directory / "pg_dump"
    subprocess.run(
        ["go", "build", "-buildvcs=false", "-overlay", str(overlay), "-o", str(binary), "."],
        cwd=REPO,
        check=True,
    )
    subprocess.run(
        ["go", "build", "-o", str(dump), str(FIXTURES / "pg_dump.go")],
        cwd=REPO,
        check=True,
    )
    return binary, dump


def verify_sql(migrations, expected):
    files = {path.name: path for path in migrations.iterdir()}
    missing = sorted(expected.keys() - files.keys())
    unexpected = sorted(files.keys() - expected.keys())
    if missing or unexpected:
        raise ValueError(f"migration directory differs (missing {missing}, unexpected {unexpected})")

    size = 0
    for name, checksum in expected.items():
        path = files[name]
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"{name} is not a regular file")
        digest = hashlib.sha256()
        with path.open("rb") as contents:
            for chunk in iter(lambda: contents.read(MIB), b""):
                digest.update(chunk)
                size += len(chunk)
        actual = digest.hexdigest()
        if actual != checksum:
            raise ValueError(f"SHA-256 mismatch for {name} (expected {checksum}, got {actual})")
    return size


def run_case(binary, dump, directory, workers, expected):
    migrations = directory / "migrations" / "public"
    migrations.mkdir(parents=True)
    for name in SEED_FILES:
        (migrations / name).touch()
    (directory / "Kmtfile.yml").write_text(
        f"""migration:
  pg_dump: {json.dumps(str(dump))}
  folder: migrations
  connections:
    fixture:
      name: fixture
      schemas:
        public: {{}}
""",
        encoding="utf-8",
    )
    env = {**os.environ, "GOMAXPROCS": str(workers), "GOGC": "100", "GOMEMLIMIT": "off"}
    argv = [str(binary), "generate", "--include-data", f"--table={TABLES}", "fixture", "public"]
    log_path = directory / "generate.log"
    with log_path.open("wb") as log:
        start = time.perf_counter()
        with subprocess.Popen(argv, cwd=directory, env=env, stdout=log, stderr=log) as process:
            # Reap this CLI directly so the measurement excludes build and verification.
            _, status, usage = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
        seconds = time.perf_counter() - start
    if process.returncode != 0:
        log = log_path.read_text(encoding="utf-8", errors="replace")
        raise ValueError(f"kmt generate exited with {process.returncode}\n{log}")

    rss_bytes = usage.ru_maxrss
    if sys.platform == "linux":
        rss_bytes *= 1024
    return seconds, rss_bytes / MIB, verify_sql(migrations, expected) / MIB


def main():
    parser = argparse.ArgumentParser(description="Benchmark KMT generate and verify its SQL.")
    parser.add_argument("--repeats", type=int, default=3, help="runs per worker count (default: 3)")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    if sys.platform not in ("darwin", "linux"):
        parser.error("peak RSS measurement requires macOS or Linux")

    expected = json.loads((FIXTURES / "sql-sha256.json").read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="kmt-benchmark-") as temporary:
        binary, dump = build_fixture(Path(temporary).resolve())
        for workers in WORKERS:
            elapsed = []
            peak_rss = []
            for repeat in range(1, args.repeats + 1):
                with tempfile.TemporaryDirectory(dir=temporary) as case:
                    try:
                        seconds, rss, size = run_case(binary, dump, Path(case), workers, expected)
                    except (OSError, ValueError) as error:
                        raise SystemExit(f"workers={workers} run={repeat} {error}") from error
                elapsed.append(seconds)
                peak_rss.append(rss)
                print(
                    f"workers={workers} run={repeat} seconds={seconds:.3f} "
                    f"peak_RSS_MiB={rss:.2f} SQL_MiB={size:.2f} "
                    f"SHA-256=ok ({len(expected) - len(SEED_FILES)} generated + {len(SEED_FILES)} seed)",
                    flush=True,
                )
            print(
                f"workers={workers} median_seconds={statistics.median(elapsed):.3f} "
                f"median_peak_RSS_MiB={statistics.median(peak_rss):.2f} "
                f"max_peak_RSS_MiB={max(peak_rss):.2f}",
                flush=True,
            )


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
