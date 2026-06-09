from datetime import date

import pandas as pd

from pipeline.organisations.import_trust_admissions import (
    ARCHIVE_PERIOD_END,
    ARCHIVE_PERIOD_START,
    discover_provisional_publications,
    filter_after_finalised,
    parse_provisional_slug,
    period_to_fy_start_year,
    parse_activity_csv,
    parse_activity_month,
    provisional_publications_to_import,
)

SAMPLE_ACTIVITY_CSV = """Financial Year,Reporting Period,Activity Month,Organisation breakdown,Org Code,Org Name,Region Code,Region Name,All specialties: Ordinary Elective,All specialties: Daycase Elective,All specialties: Elective total,All specialties: Non-Elective
2024-25,13,Mar 2025,Provider,R0A,Test Trust,-,-,50,150,200,250
2024-25,13,Feb 2025,Provider,R0A,Test Trust,-,-,40,140,180,220
2024-25,12,Oct 2024,Provider,R0A,Test Trust,-,-,30,130,160,210
2024-25,13,Mar 2025,Provider,RR8,Other Trust,-,-,10,20,30,40
2024-25,13,Mar 2025,National,All,-,-,100,200,300,400
"""

ARCHIVE_WINDOW_CSV = """Financial Year,Reporting Period,Activity Month,Organisation breakdown,Org Code,Org Name,Region Code,Region Name,All specialties: Ordinary Elective,All specialties: Daycase Elective,All specialties: Elective total,All specialties: Non-Elective
2018-19,10,Jan-19,Provider,R0A,Test Trust,-,-,1,2,3,4
2019-20,12,Mar-20,Provider,R0A,Test Trust,-,-,5,6,11,12
2020-21,1,Apr-20,Provider,R0A,Test Trust,-,-,7,8,15,16
"""


def test_period_to_fy_start_year():
    assert period_to_fy_start_year(date(2024, 4, 1)) == 2024
    assert period_to_fy_start_year(date(2025, 3, 1)) == 2024

def test_parse_activity_month_formats():
    assert parse_activity_month("Mar 2025") == pd.Timestamp("2025-03-01")
    assert parse_activity_month("Mar-24") == pd.Timestamp("2024-03-01")


def test_parse_activity_csv_m13_sample():
    df = parse_activity_csv(SAMPLE_ACTIVITY_CSV)
    assert len(df) == 4
    assert set(df["ods_code"]) == {"R0A", "RR8"}
    row = df[(df["ods_code"] == "R0A") & (df["period"] == date(2025, 3, 1))].iloc[0]
    assert row["all_specialties_elective_total"] == 200
    assert row["all_specialties_non_elective"] == 250
    assert row["all_specialties_total"] == 450


def test_parse_activity_csv_archive_window():
    df = parse_activity_csv(ARCHIVE_WINDOW_CSV)
    periods = set(df["period"])
    assert periods == {date(2019, 1, 1), date(2020, 3, 1), date(2020, 4, 1)}
    archive = df[(df["period"] >= ARCHIVE_PERIOD_START) & (df["period"] <= ARCHIVE_PERIOD_END)]
    assert set(archive["period"]) == {date(2019, 1, 1), date(2020, 3, 1)}


def test_parse_activity_csv_suppressed_counts():
    csv = ARCHIVE_WINDOW_CSV.replace("1,2,3,4", "*,*,*,*")
    row = parse_activity_csv(csv).iloc[0]
    assert row["all_specialties_ordinary_elective"] == 4  # midpoint of 1–7


SAMPLE_INDEX_HTML = """
<html><body>
<a href="https://digital.nhs.uk/data-and-information/publications/statistical/provisional-monthly-hospital-episode-statistics-for-admitted-patient-care-outpatient-and-accident-and-emergency-data/april-2025---april-2025">Apr</a>
<a href="https://digital.nhs.uk/data-and-information/publications/statistical/provisional-monthly-hospital-episode-statistics-for-admitted-patient-care-outpatient-and-accident-and-emergency-data/april-2025---january-2026">Jan</a>
<a href="https://digital.nhs.uk/data-and-information/publications/statistical/provisional-monthly-hospital-episode-statistics-for-admitted-patient-care-outpatient-and-accident-and-emergency-data/april-2026---april-2026">Open FY</a>
<a href="https://digital.nhs.uk/data-and-information/publications/statistical/provisional-monthly-hospital-episode-statistics-for-admitted-patient-care-outpatient-and-accident-and-emergency-data/april-2024---march-2025-month-13">M13</a>
</body></html>
"""


def test_parse_provisional_slug():
    assert parse_provisional_slug("april-2025---january-2026") == (2025, date(2026, 1, 1))
    assert parse_provisional_slug("april-2025---may-2026") is None
    assert parse_provisional_slug("april-2024---march-2025-month-13") is None


def test_discover_provisional_publications_picks_latest_per_fy():
    discovered = discover_provisional_publications(SAMPLE_INDEX_HTML)
    assert discovered[2025] == (
        "https://digital.nhs.uk/data-and-information/publications/statistical/"
        "provisional-monthly-hospital-episode-statistics-for-admitted-patient-care-"
        "outpatient-and-accident-and-emergency-data/april-2025---january-2026"
    )
    assert 2024 not in discovered


def test_filter_after_finalised():
    df = parse_activity_csv(SAMPLE_ACTIVITY_CSV)
    filtered = filter_after_finalised(df, date(2025, 2, 1))
    assert set(filtered["period"]) == {date(2025, 3, 1)}


def test_provisional_publications_to_import_open_fy_range():
    pubs = provisional_publications_to_import(
        date(2025, 3, 1), SAMPLE_INDEX_HTML, today=date(2026, 6, 1)
    )
    assert [fy for fy, _ in pubs] == [2025, 2026]
