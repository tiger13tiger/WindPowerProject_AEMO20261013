"""Archive backfill: daily bundles of five-minute zips, written as live bronze."""

from __future__ import annotations

import io
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import httpx
import pytest
from backfill_archive import (
    ARCHIVE_BASE,
    backfill_report,
    bundles_to_read,
    month_bounds,
    reopen_sealed_days,
    writable,
)
from ingest_scada import SCADA
from nemweb import NEM_TZ, bronze_path, load_watermark, watermark_path

SCADA_TABLE = SCADA.tables[0]
FOLDER = ARCHIVE_BASE + "Dispatch_SCADA/"


def scada_csv(stamp: datetime) -> str:
    when = stamp.strftime("%Y/%m/%d %H:%M:%S")
    return (
        "C,NEMP.WORLD,DISPATCHSCADA,AEMO,PUBLIC\n"
        "I,DISPATCH,UNIT_SCADA,1,SETTLEMENTDATE,DUID,SCADAVALUE\n"
        f'D,DISPATCH,UNIT_SCADA,1,"{when}",ARWF1,45.1\n'
        f'D,DISPATCH,UNIT_SCADA,1,"{when}",BW01,585.4\n'
        "C,END OF REPORT,4\n"
    )


def five_minute_zip(stamp: datetime) -> tuple[str, bytes]:
    name = f"PUBLIC_DISPATCHSCADA_{stamp:%Y%m%d%H%M}_0000000{stamp:%m%d%H%M}.zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as inner:
        inner.writestr(name.replace(".zip", ".CSV"), scada_csv(stamp))
    return name, buffer.getvalue()


def daily_bundle(day: date, skip: set[datetime] = frozenset()) -> bytes:
    """A market day as AEMO archives it: stamps day 04:05 .. day+1 04:00."""
    start = datetime(day.year, day.month, day.day, 4, 5, tzinfo=NEM_TZ)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as outer:
        for i in range(288):
            stamp = start + timedelta(minutes=5 * i)
            if stamp in skip:
                continue
            name, payload = five_minute_zip(stamp)
            outer.writestr(name, payload)
    return buffer.getvalue()


def client_for(days: list[date], skip: set[datetime] = frozenset()) -> httpx.Client:
    bundles = {f"PUBLIC_DISPATCHSCADA_{d:%Y%m%d}.zip": daily_bundle(d, skip) for d in days}
    listing = "".join(f'<a href="{name}">{name}</a><br>' for name in bundles)

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == FOLDER:
            return httpx.Response(200, text=listing)
        name = url.removeprefix(FOLDER)
        if name in bundles:
            return httpx.Response(200, content=bundles[name])
        return httpx.Response(404)

    return httpx.Client(transport=httpx.MockTransport(handler))


def hour(day: date, h: int) -> datetime:
    return datetime(day.year, day.month, day.day, h, tzinfo=NEM_TZ)


def rows_in(path: Path) -> int:
    return duckdb.sql(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]


AUG1, AUG2, AUG3, AUG4 = (date(2026, 8, d) for d in (1, 2, 3, 4))


def test_writes_every_hour_of_the_range_including_the_split_one(tmp_path: Path) -> None:
    # Range is Aug 2 only; Aug 1 and Aug 3 are the neighbours that complete its edge hours.
    with client_for([AUG1, AUG2, AUG3]) as client:
        totals = backfill_report("scada", AUG2, AUG2, client, bronze_dir=tmp_path)

    written = sorted(p.name for p in (tmp_path / "scada" / "date=2026-08-02").glob("*.parquet"))
    assert len(written) == 24
    assert totals["scada"].written == 24
    # Hour 04 straddles two bundles (04:00 is in Aug 1's, 04:05+ in Aug 2's).
    assert rows_in(bronze_path(SCADA_TABLE, hour(AUG2, 4), tmp_path)) == 12 * 2
    # Neighbouring days are read, never written.
    assert not (tmp_path / "scada" / "date=2026-08-01").exists()
    assert not (tmp_path / "scada" / "date=2026-08-03").exists()


def test_watermark_records_every_file_written(tmp_path: Path) -> None:
    with client_for([AUG1, AUG2, AUG3]) as client:
        backfill_report("scada", AUG2, AUG2, client, bronze_dir=tmp_path)
    assert len(load_watermark(watermark_path(SCADA_TABLE, tmp_path))) == 288


def test_existing_hours_are_never_rewritten(tmp_path: Path) -> None:
    existing = bronze_path(SCADA_TABLE, hour(AUG2, 10), tmp_path)
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"live")
    with client_for([AUG1, AUG2, AUG3]) as client:
        totals = backfill_report("scada", AUG2, AUG2, client, bronze_dir=tmp_path)
    assert existing.read_bytes() == b"live"
    assert totals["scada"].skipped_existing == 1
    assert totals["scada"].written == 23


def test_an_hour_aemo_left_short_is_written_short(tmp_path: Path) -> None:
    gap = hour(AUG2, 12) + timedelta(minutes=30)
    with client_for([AUG1, AUG2, AUG3], skip={gap}) as client:
        backfill_report("scada", AUG2, AUG2, client, bronze_dir=tmp_path)
    assert rows_in(bronze_path(SCADA_TABLE, hour(AUG2, 12), tmp_path)) == 11 * 2


def test_edge_hours_without_their_neighbour_bundle_are_not_written(tmp_path: Path) -> None:
    # Aug 1 is the oldest bundle, so Aug 1's hour 04 is missing its 04:00 interval
    # (that lives in the absent Jul 31 bundle) and must not be written partial.
    with client_for([AUG1, AUG2]) as client:
        backfill_report("scada", AUG1, AUG1, client, bronze_dir=tmp_path)
    day = tmp_path / "scada" / "date=2026-08-01"
    hours = sorted(int(p.stem.removeprefix("hour=")) for p in day.glob("*.parquet"))
    assert hours == list(range(5, 24))


def test_rerun_is_idempotent(tmp_path: Path) -> None:
    with client_for([AUG1, AUG2, AUG3]) as client:
        backfill_report("scada", AUG2, AUG2, client, bronze_dir=tmp_path)
        again = backfill_report("scada", AUG2, AUG2, client, bronze_dir=tmp_path)
    assert again["scada"].written == 0
    assert again["scada"].skipped_existing == 24
    assert len(load_watermark(watermark_path(SCADA_TABLE, tmp_path))) == 288


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    with client_for([AUG1, AUG2, AUG3]) as client:
        totals = backfill_report("scada", AUG2, AUG2, client, bronze_dir=tmp_path, dry_run=True)
    assert totals["scada"].written == 24
    assert not any(tmp_path.rglob("*.parquet"))
    assert not watermark_path(SCADA_TABLE, tmp_path).exists()


def test_missing_archive_folder_is_skipped_not_fatal(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        totals = backfill_report("scada", AUG2, AUG2, client, bronze_dir=tmp_path)
    assert totals["scada"].written == 0


def test_bundles_to_read_takes_one_neighbour_each_side() -> None:
    available = {AUG1: "a", AUG2: "b", AUG3: "c", AUG4: "d"}
    assert bundles_to_read(available, AUG2, AUG2) == ["a", "b", "c"]


def test_writable_needs_the_whole_hour_inside_what_was_read() -> None:
    h = hour(AUG2, 4)
    assert writable(h, h, h + timedelta(minutes=55))
    assert not writable(h, h + timedelta(minutes=5), h + timedelta(hours=2))
    assert not writable(h, h, h + timedelta(minutes=50))


def test_month_bounds() -> None:
    assert month_bounds("2026-08") == (date(2026, 8, 1), date(2026, 8, 31))
    assert month_bounds("2026-02") == (date(2026, 2, 1), date(2026, 2, 28))
    assert month_bounds("2026-12") == (date(2026, 12, 1), date(2026, 12, 31))


@pytest.mark.parametrize("complete, removed", [(False, True), (True, False)])
def test_reopen_removes_only_sealed_incomplete_days(
    tmp_path: Path, complete: bool, removed: bool
) -> None:
    path = tmp_path / "price" / "date=2026-10-11.parquet"
    path.parent.mkdir(parents=True)
    duckdb.sql(
        f"COPY (SELECT {str(complete).lower()} AS is_complete, 129 AS intervals_present) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    outside = tmp_path / "price" / "date=2026-09-30.parquet"
    duckdb.sql(f"COPY (SELECT false AS is_complete) TO '{outside}' (FORMAT PARQUET)")

    reopen_sealed_days(date(2026, 10, 1), date(2026, 10, 12), tmp_path)

    assert path.exists() is not removed
    assert outside.exists()  # outside the range: untouched
