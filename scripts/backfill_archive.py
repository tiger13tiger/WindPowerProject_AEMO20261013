"""Backfill bronze from AEMO's NEMWeb archive.

``Current/`` holds about two days, so the live ingest can never reach further
back than the day it first ran. ``Archive/`` holds a rolling year of daily
bundles: ``PUBLIC_DISPATCHSCADA_YYYYMMDD.zip`` is a zip of the same five-minute
zips the live ingest downloads one at a time. Unpacking those into the live
pipeline's own parser and writer means a backfilled hour is byte-for-byte the
shape of a live one, so silver, gold and the features need no special case.

A daily bundle covers a market day, which AEMO does not start at midnight, so
an hour can straddle two bundles. The bundles either side of the range are read
too, and an hour is written only once every interval it can hold has been seen.
Inside that span the archive is final, so an hour AEMO left short is written
short: nothing more will ever arrive for it.

Usage:
    python scripts/backfill_archive.py --month 2026-08
    python scripts/backfill_archive.py --start 2026-10-01 --end 2026-10-12 --report scada
    python scripts/backfill_archive.py --month 2026-09 --dry-run
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Final

import duckdb
import httpx
from ingest_dispatchis import DISPATCHIS
from ingest_p5min import P5MIN
from ingest_scada import SCADA
from nemweb import (
    BRONZE_DIR,
    HTTP_TIMEOUT,
    LOG,
    REPO_ROOT,
    USER_AGENT,
    ParseError,
    ReportSpec,
    append_watermark,
    bronze_path,
    extract_rows,
    hour_of,
    interval_from_filename,
    read_zip_payload,
    watermark_path,
    write_bronze,
)

ARCHIVE_BASE: Final = "https://nemweb.com.au/Reports/Archive/"
SILVER_DIR: Final = REPO_ROOT / "data" / "silver"
LAST_INTERVAL: Final = timedelta(minutes=55)  # an hour's files are stamped HH:00 .. HH:55

# Predispatch is left out on purpose: its Current/ directory already holds about
# fifteen days, and its archive bundles are the largest of the four by far.
ARCHIVES: Final = {
    "scada": (SCADA, "Dispatch_SCADA/"),
    "dispatchis": (DISPATCHIS, "DispatchIS_Reports/"),
    "p5min": (P5MIN, "P5_Reports/"),
}

# Silver models whose sealed-incomplete days the backfill can now complete.
SILVER_MODELS: Final = ("price", "wind_output")


@dataclass
class Totals:
    """What one report's backfill did, for the closing log line."""

    written: int = 0
    skipped_existing: int = 0
    rows: int = 0


def daily_name_re(spec: ReportSpec) -> re.Pattern[str]:
    """Match a daily archive bundle, e.g. PUBLIC_DISPATCHSCADA_20260801.zip."""
    return re.compile(rf"{spec.file_prefix}_(\d{{8}})\.zip", re.IGNORECASE)


def list_archive(spec: ReportSpec, url: str, client: httpx.Client) -> dict[date, str]:
    """Return the archive's daily bundles keyed by the day they cover."""
    response = client.get(url)
    response.raise_for_status()
    found = {
        datetime.strptime(match.group(1), "%Y%m%d").date(): match.group(0)
        for match in daily_name_re(spec).finditer(response.text)
    }
    if not found:
        excerpt = " ".join(response.text.split())[:200]
        raise ParseError(f"no daily {spec.file_prefix} bundles in {url}: {excerpt!r}")
    return dict(sorted(found.items()))


def bundles_to_read(available: dict[date, str], start: date, end: date) -> list[str]:
    """Bundles covering the range plus one day either side, oldest first.

    The neighbours are there for the hours a market-day boundary splits; they
    are read, but only hours inside [start, end] are ever written.
    """
    first, last = start - timedelta(days=1), end + timedelta(days=1)
    return [name for day, name in available.items() if first <= day <= last]


def inner_files(spec: ReportSpec, bundle: bytes, bundle_name: str) -> Iterator[tuple[str, str]]:
    """Yield (five-minute file name, CSV text) from one daily bundle.

    Bundles are zips of zips. A member that is already a CSV is accepted too,
    so a change of packaging on AEMO's side fails loudly in the parser rather
    than silently yielding nothing.
    """
    with zipfile.ZipFile(io.BytesIO(bundle)) as archive:
        members = sorted(archive.namelist())
        matched = 0
        for member in members:
            name = Path(member).name
            stem, _, suffix = name.rpartition(".")
            is_report_file = spec.filename_re.fullmatch(stem + ".zip")
            if suffix.lower() not in ("zip", "csv") or not is_report_file:
                continue
            matched += 1
            payload = archive.read(member)
            if suffix.lower() == "zip":
                yield name, read_zip_payload(payload, name)
            else:
                yield stem + ".zip", payload.decode("utf-8", errors="replace")
        if not matched:
            raise ParseError(
                f"{bundle_name} holds no {spec.file_prefix} five-minute files; "
                f"first members: {members[:5]}"
            )


def writable(hour: datetime, first_seen: datetime, last_seen: datetime) -> bool:
    """True once every interval the hour can hold lies inside what has been read."""
    return first_seen <= hour and hour + LAST_INTERVAL <= last_seen


def in_range(hour: datetime, start: date, end: date) -> bool:
    return start <= hour.date() <= end


def write_hour(
    spec: ReportSpec,
    hour: datetime,
    texts: list[tuple[str, str]],
    totals: dict[str, Totals],
    done: dict[str, list[str]],
    bronze_dir: Path,
    dry_run: bool,
) -> None:
    """Write one hour for every table of the report that does not have it yet."""
    names = [name for name, _ in texts]
    for table in spec.tables:
        destination = bronze_path(table, hour, bronze_dir)
        if destination.exists():
            # Write-once: the live ingest or an earlier backfill already has it.
            totals[table.key].skipped_existing += 1
            continue
        if dry_run:
            LOG.info("[%s] would write %s from %d files", table.key,
                     destination.relative_to(bronze_dir), len(names))
            totals[table.key].written += 1
            continue
        try:
            rows: list[tuple] = []
            for name, text in texts:
                rows.extend(extract_rows(table, text, name))
            count = write_bronze(table, rows, destination)
        except ParseError as exc:
            LOG.warning("[%s] skipping hour %s: %s", table.key, f"{hour:%Y-%m-%d %H}", exc)
            continue
        totals[table.key].written += 1
        totals[table.key].rows += count
        done[table.key].extend(names)


def backfill_report(
    key: str,
    start: date,
    end: date,
    client: httpx.Client,
    bronze_dir: Path = BRONZE_DIR,
    dry_run: bool = False,
) -> dict[str, Totals]:
    """Backfill every table of one report for NEM dates start..end inclusive."""
    spec, folder = ARCHIVES[key]
    url = ARCHIVE_BASE + folder
    totals = {table.key: Totals() for table in spec.tables}
    try:
        available = list_archive(spec, url, client)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            raise
        # Not every report is archived. Say so and move on: the others still count.
        LOG.warning("[%s] no archive at %s; skipped", key, url)
        return totals
    bundles = bundles_to_read(available, start, end)
    LOG.info("[%s] archive holds %s .. %s; reading %d bundles for %s .. %s",
             key, min(available), max(available), len(bundles), start, end)
    if not bundles:
        LOG.warning("[%s] nothing in the archive for %s .. %s", key, start, end)

    done: dict[str, list[str]] = {table.key: [] for table in spec.tables}
    pending: dict[datetime, list[tuple[str, str]]] = {}
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    dropped: list[datetime] = []

    def flush(final: bool) -> None:
        assert first_seen is not None and last_seen is not None
        for hour in sorted(pending):
            if writable(hour, first_seen, last_seen):
                write_hour(spec, hour, pending.pop(hour), totals, done, bronze_dir, dry_run)
            elif final or first_seen > hour:
                # Its first intervals were in a bundle the archive no longer holds.
                pending.pop(hour)
                dropped.append(hour)

    for bundle_name in bundles:
        response = client.get(url + bundle_name)
        response.raise_for_status()
        count = 0
        for name, text in inner_files(spec, response.content, bundle_name):
            stamp = interval_from_filename(spec, name)
            first_seen = stamp if first_seen is None else min(first_seen, stamp)
            if last_seen is not None and hour_of(stamp) > hour_of(last_seen):
                flush(final=False)  # write each hour as it closes: P5MIN files are large
            last_seen = stamp if last_seen is None else max(last_seen, stamp)
            hour = hour_of(stamp)
            if in_range(hour, start, end):
                pending.setdefault(hour, []).append((name, text))
            count += 1
        LOG.info("[%s] %s: %d five-minute files", key, bundle_name, count)
    if first_seen is not None:
        flush(final=True)
    if dropped:
        LOG.warning("[%s] %d hour(s) in range not fully covered by the archive, not written: %s",
                    key, len(dropped), ", ".join(f"{h:%Y-%m-%d %H}:00" for h in dropped[:6]))

    if not dry_run:
        for table in spec.tables:
            append_watermark(done[table.key], watermark_path(table, bronze_dir))
    for table in spec.tables:
        t = totals[table.key]
        LOG.info("[%s] %s %d hours (%d rows), %d already present",
                 table.key, "would write" if dry_run else "wrote",
                 t.written, t.rows, t.skipped_existing)
    return totals


def reopen_sealed_days(start: date, end: date, silver_dir: Path = SILVER_DIR) -> list[Path]:
    """Remove sealed-incomplete silver days in the range so silver rebuilds them.

    A day is sealed incomplete because Current/ no longer holds its missing
    files. The archive does, so once the backfill has supplied them the seal's
    premise is gone. This is the one place a silver file is ever removed, and
    only one that says of itself that it is incomplete.
    """
    removed: list[Path] = []
    for model in SILVER_MODELS:
        for path in sorted((silver_dir / model).glob("date=*.parquet")):
            day = date.fromisoformat(path.stem.removeprefix("date="))
            if not start <= day <= end:
                continue
            literal = str(path).replace("'", "''")
            con = duckdb.connect()
            try:
                incomplete = con.execute(
                    "SELECT count(*) FILTER (WHERE NOT is_complete) > 0 "
                    f"FROM read_parquet('{literal}')"
                ).fetchone()
            finally:
                con.close()
            if incomplete and incomplete[0]:
                path.unlink()
                removed.append(path)
                LOG.info("[silver] reopened %s: it was sealed incomplete",
                         path.relative_to(silver_dir))
    return removed


def month_bounds(month: str) -> tuple[date, date]:
    """Return the first and last day of a YYYY-MM month."""
    first = datetime.strptime(month, "%Y-%m").date()
    following = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    return first, following - timedelta(days=1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill bronze from AEMO's NEMWeb archive.")
    span = parser.add_mutually_exclusive_group(required=True)
    span.add_argument("--month", help="YYYY-MM: backfill that whole month")
    span.add_argument("--start", type=date.fromisoformat, help="first NEM date, YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, help="last NEM date (with --start)")
    parser.add_argument("--report", choices=[*ARCHIVES, "all"], default="all")
    parser.add_argument("--dry-run", action="store_true", help="list what would be written")
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.month:
        start, end = month_bounds(args.month)
    else:
        if args.end is None:
            parser.error("--start needs --end")
        start, end = args.start, args.end
    if end < start:
        parser.error("--end is before --start")

    keys = list(ARCHIVES) if args.report == "all" else [args.report]
    failed = []
    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=headers, follow_redirects=True) as client:
        for key in keys:
            try:
                backfill_report(key, start, end, client, args.bronze_dir, args.dry_run)
            except (httpx.HTTPError, zipfile.BadZipFile, ParseError) as exc:
                # Reports are independent, as in the live ingest.
                LOG.error("[%s] backfill failed: %s", key, exc)
                failed.append(key)

    # Silver joins generation to price, so a day is only worth reopening when
    # both were backfilled in this run.
    if not args.dry_run and {"scada", "dispatchis"} <= set(keys) - set(failed):
        reopen_sealed_days(start, end, args.silver_dir)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
