"""Tests for the wind DUID registry extractor.

The registration list is an AEMO spreadsheet whose layout shifts between
editions, so these tests exercise the things that actually break: the header
not being on the first row, columns being renamed, and rows that are not wind.
"""

import io
import json

import pytest

openpyxl = pytest.importorskip("openpyxl", reason="reference extras not installed")
pytest.importorskip("pandas", reason="reference extras not installed")

from refresh_reference import (  # noqa: E402
    BATTERY_HEADER,
    UNITS_HEADER,
    RegistryError,
    annotate_in_scada,
    battery_rows,
    extract_units,
    extract_wind,
    fuel_group,
    is_generation,
    normalise_capacity,
    normalise_region,
    scada_duids,
    write_csv,
    write_meta,
)

HEADERS = [
    "Participant",
    "Station Name",
    "Region",
    "Dispatch Type",
    "Schedule Type",
    "Fuel Source - Descriptor",
    "Technology Type - Descriptor",
    "DUID",
    "Reg Cap (MW)",
]

# Row 7 is shaped like Hornsdale Power Reserve: AEMO files the battery under
# fuel source "Wind" because it sits beside the wind farm. Only the technology
# and dispatch columns give it away.
ROWS = [
    ["Acme", "Example Wind Farm", "SA1", "Generator", "Semi-Scheduled",
     "Wind", "Wind - Onshore", "EXWF1", 150.0],
    ["Acme", "Example Wind Farm", "SA1", "Generator", "Semi-Scheduled",
     "Wind", "Wind - Onshore", "EXWF2", 90.5],
    ["Beta", "Coal Creek", "NSW1", "Generator", "Scheduled",
     "Black Coal", "Steam Sub Critical", "COAL1", 700.0],
    ["Gamma", "Sunny Solar", "QLD1", "Generator", "Semi-Scheduled",
     "Solar", "Photovoltaic", "SOLAR1", 200.0],
    ["Delta", "Hill Wind", "VIC1", "Generator", "Semi-Scheduled",
     "Wind - Onshore", "Wind", "HILLWF1", 320.0],
    # Registered wind, dispatchable, but NOT semi-scheduled — so absent from
    # AEMO's availability forecast. This row is why the unfiltered comparison
    # showed Tasmanian output above its own availability.
    ["Epsilon", "Island Wind", "TAS1", "Generator", "Non-Scheduled",
     "WIND", "Wind - Onshore", "ISLWF1", ""],
    ["Zeta", "Offshore Trial", "", "Generator", "Semi-Scheduled",
     "Wind", "Wind - Offshore", "NOREGION1", 50.0],
    ["Neoen", "Hornsdale Power Reserve", "SA1", "Generator", "Scheduled",
     "Wind", "Battery", "HPR1", 150.0],
    ["Eta", "Big Battery", "VIC1", "Load", "Scheduled",
     "Battery storage", "Battery Storage", "BATT1", 100.0],
    ["Theta", "New BDU", "NSW1", "Bidirectional", "Scheduled",
     "Wind", "", "BDU1", 80.0],
]


def workbook_bytes(headers=HEADERS, rows=ROWS, sheet="Generators and Scheduled Loads") -> bytes:
    """Build a workbook shaped like AEMO's: title rows above the real header."""
    book = openpyxl.Workbook()
    page = book.active
    page.title = sheet
    page.append(["NEM Registration and Exemption List"])  # title
    page.append([])  # blank spacer
    page.append(headers)
    for row in rows:
        page.append(row)

    # A second sheet, so sheet selection is actually exercised.
    book.create_sheet("Notes").append(["not the table you want"])

    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def test_extracts_only_wind_rows():
    rows = extract_wind(workbook_bytes()).kept
    assert [row["duid"] for row in rows] == ["EXWF1", "EXWF2", "HILLWF1", "ISLWF1"]


def test_excludes_coal_solar_and_storage():
    duids = {row["duid"] for row in extract_wind(workbook_bytes()).kept}
    assert duids.isdisjoint({"COAL1", "SOLAR1", "BATT1"})


def test_hornsdale_power_reserve_is_not_a_wind_farm():
    # The regression this exists for: HPR1 shipped in the registry because its
    # fuel source descriptor reads "Wind". It is a battery.
    result = extract_wind(workbook_bytes())
    assert "HPR1" not in {row["duid"] for row in result.kept}
    assert "HPR1" in {row["duid"] for row in result.excluded}


def test_bidirectional_units_are_excluded():
    # Newer registrations use a Bidirectional dispatch type instead of a
    # Generator/Load pair, with nothing in the technology column.
    assert "BDU1" not in {row["duid"] for row in extract_wind(workbook_bytes()).kept}


def test_exclusions_are_reported_with_enough_detail_to_check():
    excluded = {row["duid"]: row for row in extract_wind(workbook_bytes()).excluded}
    assert excluded["HPR1"]["station_name"] == "Hornsdale Power Reserve"
    assert excluded["HPR1"]["fuel_type"] == "Wind"
    assert excluded["HPR1"]["technology_type"] == "Battery"


@pytest.mark.parametrize(
    ("fuel", "technology", "dispatch", "expected"),
    [
        ("Wind", "Wind - Onshore", "Generator", True),
        ("Wind", "Battery Storage", "Generator", False),
        ("Wind", "", "Bidirectional", False),
        ("Wind", "", "Bi-Directional", False),
        ("Wind", "BESS", "Generator", False),
        ("Wind", "", "Load", False),
        ("Wind", "Pump Storage", "Generator", False),
        ("Wind - Offshore", "", "Generator", True),
    ],
)
def test_is_generation(fuel, technology, dispatch, expected):
    assert is_generation(fuel, technology, dispatch) is expected


def test_drops_rows_without_a_nem_region():
    # A wind row with no region cannot be joined to a price, so it is dropped.
    assert "NOREGION1" not in {row["duid"] for row in extract_wind(workbook_bytes()).kept}


def test_captures_station_region_and_capacity():
    row = next(r for r in extract_wind(workbook_bytes()).kept if r["duid"] == "HILLWF1")
    assert row["station_name"] == "Hill Wind"
    assert row["region"] == "VIC1"
    assert row["fuel_type"] == "Wind - Onshore"
    assert row["capacity_mw"] == 320.0


def test_missing_capacity_becomes_none_not_zero():
    # Zero would read as a real 0 MW farm and silently break capacity factors.
    row = next(r for r in extract_wind(workbook_bytes()).kept if r["duid"] == "ISLWF1")
    assert row["capacity_mw"] is None


def test_matches_fuel_case_insensitively():
    assert "ISLWF1" in {row["duid"] for row in extract_wind(workbook_bytes()).kept}  # "WIND"


def test_finds_the_header_even_when_columns_are_renamed():
    # Indexed by name, not position: AEMO inserts columns between editions and
    # so do we, and a positional fixture breaks on both.
    headers = list(HEADERS)
    headers[HEADERS.index("Fuel Source - Descriptor")] = "Fuel Source - Primary"
    headers[HEADERS.index("Reg Cap (MW)")] = "Registered Capacity (MW)"
    rows = extract_wind(workbook_bytes(headers=headers)).kept
    assert len(rows) == 4


def test_reports_what_it_saw_when_a_column_is_missing():
    headers = [h for h in HEADERS if h != "Reg Cap (MW)"] + ["Something Else"]
    rows = [row[:7] + ["x"] for row in ROWS]
    with pytest.raises(RegistryError, match="columns not found"):
        extract_wind(workbook_bytes(headers=headers, rows=rows))


def test_rejects_a_workbook_with_no_wind():
    fuel = HEADERS.index("Fuel Source - Descriptor")
    rows = [r for r in ROWS if "ind" not in str(r[fuel]).lower()]
    with pytest.raises(RegistryError, match="no wind rows"):
        extract_wind(workbook_bytes(rows=rows))


def test_rejects_a_workbook_with_no_duid_column():
    with pytest.raises(RegistryError, match="no sheet looks like|no header row"):
        extract_wind(workbook_bytes(headers=["A", "B", "C"], rows=[[1, 2, 3]], sheet="Notes"))


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("SA1", "SA1"), ("sa1", "SA1"), (" NSW1 ", "NSW1"), ("VIC", "VIC1"), ("WA", None), ("", None)],
)
def test_normalise_region(raw, expected):
    assert normalise_region(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("150", 150.0), ("150.5 MW", 150.5), ("", None), ("-", None), ("0", None), ("n/a", None)],
)
def test_normalise_capacity(raw, expected):
    assert normalise_capacity(raw) == expected


def test_csv_is_written_sorted_with_a_stable_header(tmp_path):
    destination = tmp_path / "wind_duids.csv"
    write_csv(extract_wind(workbook_bytes()).kept, destination)

    lines = destination.read_text().splitlines()
    assert lines[0] == ("duid,station_name,region,fuel_type,capacity_mw,"
                        "dispatch_type,schedule_type,in_scada")
    assert [line.split(",")[0] for line in lines[1:]] == ["EXWF1", "EXWF2", "HILLWF1", "ISLWF1"]


def test_schedule_type_is_kept_and_is_not_dispatch_type():
    """The two columns say different things and were once confused for each other.

    `dispatch_type` is Generator / Load / Bidirectional, which is why
    is_generation() uses it. The scheduling classification — Scheduled,
    Semi-Scheduled, Non-Scheduled — is a separate column, and it is the one
    curtailment needs: AEMO's SS_WIND_UIGF covers semi-scheduled units only.
    """
    rows = {row["duid"]: row for row in extract_wind(workbook_bytes()).kept}
    assert rows["EXWF1"]["dispatch_type"] == "Generator"
    assert rows["EXWF1"]["schedule_type"] == "Semi-Scheduled"
    # The old Tasmanian farm: registered wind, dispatchable, but not
    # semi-scheduled, so it is absent from the availability forecast. This row
    # is why the unfiltered comparison showed output above availability.
    assert rows["ISLWF1"]["dispatch_type"] == "Generator"
    assert rows["ISLWF1"]["schedule_type"] == "Non-Scheduled"


def test_schedule_type_is_matched_by_any_of_its_known_spellings():
    """AEMO renames this column between editions, and we cannot see the live one."""
    for spelling in ("Schedule Type", "Classification", "Scheduled Status"):
        headers = list(HEADERS)
        headers[HEADERS.index("Schedule Type")] = spelling
        rows = {r["duid"]: r for r in extract_wind(workbook_bytes(headers=headers)).kept}
        assert rows["EXWF1"]["schedule_type"] == "Semi-Scheduled", spelling


def test_a_missing_schedule_column_is_blank_and_warned_about(caplog):
    """Not fatal — every other consumer works without it — but never silent.

    A blank is honest. Defaulting to "Scheduled" would make every farm from an
    older edition look wrongly classified to a downstream filter.
    """
    headers = [h for h in HEADERS if h != "Schedule Type"]
    idx = HEADERS.index("Schedule Type")
    rows = [r[:idx] + r[idx + 1:] for r in ROWS]
    with caplog.at_level("WARNING"):
        kept = extract_wind(workbook_bytes(headers=headers, rows=rows)).kept
    assert all(row["schedule_type"] == "" for row in kept)
    assert "curtailment needs it" in caplog.text


def test_meta_records_provenance(tmp_path):
    payload = workbook_bytes()
    rows, excluded = extract_wind(payload)
    destination = tmp_path / "wind_duids.meta.json"
    write_meta(rows, payload, "https://example.invalid/list.xls", destination, excluded)

    meta = json.loads(destination.read_text())
    assert meta["wind_duids"] == 4
    assert {e["duid"] for e in meta["excluded_non_generation"]} == {"HPR1", "BDU1"}
    assert meta["regions"] == ["SA1", "TAS1", "VIC1"]
    assert meta["total_capacity_mw"] == 560.5  # ISLWF1 has none
    assert len(meta["source_sha256"]) == 64
    assert meta["source_url"].startswith("https://")


# --- in_scada flag --------------------------------------------------------


def _bronze_with(duids, tmp_path):
    """Write a minimal bronze SCADA tree holding the given DUIDs."""
    import duckdb

    destination = tmp_path / "scada" / "date=2026-10-13" / "hour=00.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    values = ", ".join(f"('{duid}')" for duid in duids)
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t(duid)) "
            f"TO '{destination}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    return tmp_path


def test_scada_duids_reads_the_bronze_tree(tmp_path):
    _bronze_with(["EXWF1", "HILLWF1", "COAL1"], tmp_path)
    assert scada_duids(tmp_path) == {"EXWF1", "HILLWF1", "COAL1"}


def test_scada_duids_is_none_when_bronze_is_empty(tmp_path):
    # Unknown, not "absent from SCADA" — the distinction the flag exists for.
    assert scada_duids(tmp_path) is None


def test_annotate_marks_units_present_in_scada(tmp_path):
    rows = extract_wind(workbook_bytes()).kept
    annotate_in_scada(rows, _bronze_with(["EXWF1", "HILLWF1"], tmp_path) and scada_duids(tmp_path))

    flags = {row["duid"]: row["in_scada"] for row in rows}
    assert flags == {"EXWF1": True, "EXWF2": False, "HILLWF1": True, "ISLWF1": False}


def test_non_scheduled_farms_are_false_not_missing(tmp_path):
    # A registered farm that never appears in DISPATCHSCADA is non-scheduled:
    # a property of the unit, not a gap in ingestion.
    rows = extract_wind(workbook_bytes()).kept
    _bronze_with(["EXWF1"], tmp_path)
    annotate_in_scada(rows, scada_duids(tmp_path))

    absent = [row["duid"] for row in rows if row["in_scada"] is False]
    assert absent == ["EXWF2", "HILLWF1", "ISLWF1"]


def test_unknown_in_scada_stays_none(tmp_path):
    rows = extract_wind(workbook_bytes()).kept
    annotate_in_scada(rows, scada_duids(tmp_path))
    assert all(row["in_scada"] is None for row in rows)


def test_csv_writes_lowercase_booleans_and_blank_for_unknown(tmp_path):
    rows = extract_wind(workbook_bytes()).kept
    _bronze_with(["EXWF1"], tmp_path / "bronze")
    annotate_in_scada(rows, scada_duids(tmp_path / "bronze"))
    rows[1]["in_scada"] = None  # simulate an unknown alongside known values

    destination = tmp_path / "wind_duids.csv"
    write_csv(rows, destination)
    lines = destination.read_text().splitlines()

    assert lines[1].endswith(",true")
    assert lines[2].endswith(",")  # blank, not "None"
    assert lines[3].endswith(",false")


def test_meta_counts_scada_coverage(tmp_path):
    payload = workbook_bytes()
    rows, excluded = extract_wind(payload)
    _bronze_with(["EXWF1", "HILLWF1"], tmp_path / "bronze")
    annotate_in_scada(rows, scada_duids(tmp_path / "bronze"))

    destination = tmp_path / "meta.json"
    write_meta(rows, payload, "https://example.invalid/list.xls", destination, excluded)
    meta = json.loads(destination.read_text())

    assert meta["in_scada"] == 2
    assert meta["not_in_scada"] == 2


# --- Typed unit list and battery registry -----------------------------------

PAIR_ROWS = [
    # An older battery registered as two DUIDs: one to discharge, one to charge.
    ["Iota", "Lake Battery", "QLD1", "Generator", "Scheduled",
     "Battery storage", "Battery Storage", "LAKEG1", 50.0],
    ["Iota", "Lake Battery", "QLD1", "Load", "Scheduled",
     "Battery storage", "Battery Storage", "LAKEL1", 50.0],
    # Pumped hydro stores energy too, but it is hydro, not a battery.
    ["Kappa", "Pump Hill", "NSW1", "Load", "Scheduled",
     "Water", "Hydro - Pump", "PUMP1", 240.0],
]


def test_every_unit_with_a_region_is_classified():
    units = {unit["duid"]: unit["fuel_group"] for unit in extract_units(workbook_bytes())}
    assert units == {
        "EXWF1": "wind", "EXWF2": "wind", "HILLWF1": "wind", "ISLWF1": "wind",
        "COAL1": "other", "SOLAR1": "solar",
        "HPR1": "battery", "BATT1": "battery", "BDU1": "battery",
    }  # NOREGION1 has no NEM region, so it is not a NEM unit


def test_battery_filed_under_wind_is_a_battery():
    unit = next(u for u in extract_units(workbook_bytes()) if u["duid"] == "HPR1")
    assert unit["fuel_group"] == "battery"
    assert unit["fuel_type"] == "Wind"  # AEMO's own label is kept verbatim


def test_generator_and_load_duids_of_one_battery_share_a_station_id():
    units = extract_units(workbook_bytes(rows=ROWS + PAIR_ROWS))
    batteries = {b["duid"]: b for b in battery_rows(units)}
    assert batteries["LAKEG1"]["station_id"] == batteries["LAKEL1"]["station_id"] == "lake-battery"
    assert batteries["LAKEL1"]["dispatch_type"] == "Load"
    assert "PUMP1" not in batteries


@pytest.mark.parametrize(
    "fuel, technology, dispatch, expected",
    [
        ("Wind", "Wind - Onshore", "Generator", "wind"),
        ("Wind", "Battery", "Generator", "battery"),
        ("Wind", "", "Bidirectional", "battery"),
        ("Battery storage", "Battery Storage", "Load", "battery"),
        ("Solar", "Photovoltaic", "Generator", "solar"),
        ("Water", "Hydro - Pump", "Load", "other"),
        ("Black Coal", "Steam Sub Critical", "Generator", "other"),
        ("Natural Gas", "OCGT", "Generator", "other"),
    ],
)
def test_fuel_group(fuel, technology, dispatch, expected):
    assert fuel_group(fuel, technology, dispatch) == expected


def test_storage_mwh_is_read_when_the_column_exists():
    headers = [*HEADERS, "Max Storage Capacity (MWh)"]
    rows = [[*row, 200.0 if row[7] == "BATT1" else ""] for row in ROWS]
    units = {u["duid"]: u for u in extract_units(workbook_bytes(headers=headers, rows=rows))}
    assert units["BATT1"]["storage_mwh"] == 200.0
    assert units["HPR1"]["storage_mwh"] is None


def test_storage_mwh_is_blank_when_the_column_is_absent():
    assert all(u["storage_mwh"] is None for u in extract_units(workbook_bytes()))


def test_battery_and_unit_files_have_stable_headers(tmp_path):
    units = extract_units(workbook_bytes())
    write_csv(units, tmp_path / "units.csv", UNITS_HEADER)
    write_csv(battery_rows(units), tmp_path / "battery_duids.csv", BATTERY_HEADER)
    assert (tmp_path / "units.csv").read_text().splitlines()[0] == ",".join(UNITS_HEADER)
    battery_lines = (tmp_path / "battery_duids.csv").read_text().splitlines()
    assert battery_lines[0] == ",".join(BATTERY_HEADER)
    assert [line.split(",")[0] for line in battery_lines[1:]] == ["BATT1", "BDU1", "HPR1"]


def test_wind_registry_is_unchanged_by_the_unit_list():
    # The typed list is additive: wind_duids.csv keeps exactly the rows it had.
    assert [r["duid"] for r in extract_wind(workbook_bytes()).kept] == [
        "EXWF1", "EXWF2", "HILLWF1", "ISLWF1",
    ]
