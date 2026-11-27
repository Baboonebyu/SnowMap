"""Download, extract, and load a date range of Utah SNODAS data."""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_ROOT = "https://noaadata.apps.nsidc.org/NOAA/G02158/unmasked"
DEFAULT_BOUNDARY = ROOT / "data" / "raw" / "cb_2025_us_state_500k.zip"
DEFAULT_RAW_DIR = ROOT / "data" / "raw" / "snodas"
DEFAULT_PROCESSED_DIR = ROOT / "data" / "processed" / "snodas"
DEFAULT_MANIFEST = ROOT / "data" / "processed" / "snodas_backfill_manifest.csv"
MANIFEST_FIELDS = ("date", "status", "rows", "source_url", "archive_cached")


def dates_between(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def archive_url(day: date) -> str:
    month_folder = day.strftime("%m_%b")
    filename = f"SNODAS_unmasked_{day:%Y%m%d}.tar"
    return f"{ARCHIVE_ROOT}/{day:%Y}/{month_folder}/{filename}"


def load_manifest(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != list(MANIFEST_FIELDS):
            raise ValueError(f"Manifest has an unexpected schema: {path}")
        return {row["date"]: row for row in reader}


def save_manifest(path: Path, records: dict[str, dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        for day in sorted(records):
            writer.writerow(records[day])


def is_tar(path: Path) -> bool:
    try:
        with tarfile.open(path, "r:"):
            return True
    except (tarfile.TarError, OSError):
        return False


def download_archive(url: str, destination: Path) -> tuple[bool, bool]:
    """Return (downloaded_now, source_missing); retain partial files only on success."""
    if destination.exists():
        if not is_tar(destination):
            raise ValueError(f"Existing archive is not a readable tar: {destination}")
        return False, False

    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    request = Request(url, headers={"User-Agent": "SnowMap/1.0"})
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urlopen(request, timeout=120) as response, partial.open("wb") as output:
                while True:
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    output.write(block)
            if not is_tar(partial):
                raise RuntimeError("Downloaded file was not a readable tar archive")
            partial.replace(destination)
            return True, False
        except HTTPError as error:
            partial.unlink(missing_ok=True)
            if error.code == 404:
                return False, True
            last_error = RuntimeError(f"Archive server returned HTTP {error.code}")
        except (URLError, TimeoutError, OSError, RuntimeError) as error:
            partial.unlink(missing_ok=True)
            last_error = RuntimeError(f"Archive download failed ({type(error).__name__})")
        if attempt < 2:
            time.sleep(2 ** attempt)
    raise last_error or RuntimeError("Archive download failed")


def run_command(command: list[str]) -> str:
    for attempt in range(3):
        try:
            result = subprocess.run(
                command,
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            break
        except PermissionError:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    if result.returncode:
        # Do not copy arbitrary process output into logs or the manifest.
        raise RuntimeError(f"Pipeline command failed with exit code {result.returncode}")
    return result.stdout


def process_day(day: date, index: int, total: int, args: argparse.Namespace) -> dict[str, str]:
    day_key = day.isoformat()
    url = archive_url(day)
    archive = args.raw_dir / f"SNODAS_unmasked_{day:%Y%m%d}.tar"
    output_csv = args.processed_dir / f"snodas_utah_{day:%Y%m%d}.csv"
    downloaded_now = False
    print(f"[{index}/{total}] {day_key}: starting", flush=True)
    try:
        downloaded_now, source_missing = download_archive(url, archive)
        if source_missing:
            print(f"[{index}/{total}] {day_key}: no source archive; skipped", flush=True)
            return {
                "date": day_key, "status": "missing_source", "rows": "0",
                "source_url": url, "archive_cached": "false",
            }

        run_command([
            sys.executable, str(ROOT / "scripts" / "extract_snodas_utah.py"),
            "--archive", str(archive), "--boundary-zip", str(args.boundary_zip),
            "--output", str(output_csv),
        ])
        output = run_command([
            sys.executable, str(ROOT / "scripts" / "load_bigquery.py"),
            "snodas", str(output_csv), "--project", args.project,
            "--dataset", args.dataset,
        ])
        match = re.search(r"Loaded ([\d,]+) cell-date rows", output)
        row_count = match.group(1).replace(",", "") if match else ""
        if not args.keep_csv:
            output_csv.unlink(missing_ok=True)
        if downloaded_now and not args.keep_archives:
            archive.unlink(missing_ok=True)
        print(f"[{index}/{total}] {day_key}: loaded {row_count or 'unknown'} rows", flush=True)
        return {
            "date": day_key, "status": "loaded", "rows": row_count,
            "source_url": url,
            "archive_cached": str(not downloaded_now).lower(),
        }
    except Exception as error:
        print(f"[{index}/{total}] {day_key}: failed ({type(error).__name__})", flush=True)
        return {
            "date": day_key, "status": f"failed_{type(error).__name__}",
            "rows": "", "source_url": url,
            "archive_cached": str(not downloaded_now).lower(),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, type=date.fromisoformat,
                        help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, type=date.fromisoformat,
                        help="Inclusive end date (YYYY-MM-DD)")
    parser.add_argument("--project", default="snowmap-515822")
    parser.add_argument("--dataset", default="snowmap")
    parser.add_argument("--boundary-zip", type=Path, default=DEFAULT_BOUNDARY)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--processed-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--resume", action="store_true",
                        help="Skip dates already marked loaded in the manifest")
    parser.add_argument("--workers", type=int, default=2,
                        help="Number of dates to download, extract, and load concurrently (default: 2)")
    parser.add_argument("--keep-archives", action="store_true",
                        help="Keep archives downloaded by this run after successful loads")
    parser.add_argument("--keep-csv", action="store_true",
                        help="Keep extracted daily CSVs after successful loads")
    args = parser.parse_args()

    if args.end < args.start:
        parser.error("--end must be on or after --start")
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if not args.boundary_zip.is_file():
        parser.error(f"Boundary ZIP does not exist: {args.boundary_zip}")

    records = load_manifest(args.manifest)
    args.raw_dir.mkdir(parents=True, exist_ok=True)
    args.processed_dir.mkdir(parents=True, exist_ok=True)

    total = (args.end - args.start).days + 1
    pending = []
    for index, day in enumerate(dates_between(args.start, args.end), start=1):
        if args.resume and records.get(day.isoformat(), {}).get("status") == "loaded":
            print(f"[{index}/{total}] {day.isoformat()}: already loaded; skipped", flush=True)
        else:
            pending.append((index, day))

    failures = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_day, day, index, total, args): day
            for index, day in pending
        }
        for future in as_completed(futures):
            result = future.result()
            records[result["date"]] = result
            save_manifest(args.manifest, records)
            failures += result["status"].startswith("failed_")

    loaded = sum(1 for row in records.values() if row["status"] == "loaded")
    missing = sum(1 for row in records.values() if row["status"] == "missing_source")
    print(f"Backfill complete: {loaded} loaded dates; {missing} dates had no source archive")
    print(f"Manifest: {args.manifest}")
    if failures:
        raise SystemExit(f"{failures} date(s) failed; rerun with --resume after resolving the issue")


if __name__ == "__main__":
    main()
