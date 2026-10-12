import csv
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from pipeline.utils.utils import get_bigquery_client

ROOT = Path(__file__).resolve().parents[2]
QUERIES_DIR = ROOT / "sample_data" / "queries"
OUTPUT_DIR = ROOT / "sample_data"
QUERY_TIMEOUT_SECONDS = 600


def load_queries(queries_dir=QUERIES_DIR):
    """Return (csv name, query) pairs from the SQL files in queries_dir.

    queries/<name>.sql writes <name>.csv. A file whose name starts with an
    underscore is a fragment. A query includes the fragment _<name>.sql where
    it has the text {{ <name> }}. A fragment may include another fragment.
    _scope.sql defines the month window, the products, and the trusts.
    """
    queries_dir = Path(queries_dir)
    fragments = {
        path.stem[1:]: path.read_text().strip()
        for path in queries_dir.glob("_*.sql")
    }
    queries = []
    for path in sorted(queries_dir.glob("[!_]*.sql")):
        query = path.read_text().strip().removesuffix(";").rstrip()
        query = _apply_fragments(query, fragments, path.name)
        queries.append((path.stem + ".csv", query))
    if not queries:
        raise ValueError(f"There are no query files in {queries_dir}.")
    return queries


def _apply_fragments(query, fragments, filename):
    for _ in range(len(fragments) + 1):
        if "{{" not in query:
            return query
        previous = query
        for name, fragment in fragments.items():
            query = query.replace("{{ " + name + " }}", fragment)
        if query == previous:
            break
    if "{{" in query:
        raise ValueError(f"{filename} has an unknown fragment.")
    return query


def format_cell(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        if value.hour == value.minute == value.second == value.microsecond == 0:
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (Decimal, float)):
        return _trim_number(format(value, "f"))
    if isinstance(value, (list, dict)):
        raise TypeError(f"Sample extract value {value!r} is not a single cell.")
    return str(value)


def write_csv(path, field_names, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=field_names, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({name: format_cell(row[name]) for name in field_names})
    temporary.replace(path)


def dump_sample_data(queries_dir=QUERIES_DIR, output_dir=OUTPUT_DIR, client=None):
    queries = load_queries(queries_dir)
    if client is None:
        client = get_bigquery_client()
    written = []
    for filename, query in queries:
        print(f"Running {filename}...", flush=True)
        field_names, rows = fetch_query(client, query)
        destination = Path(output_dir) / filename
        write_csv(destination, field_names, rows)
        print(f"Wrote {filename} ({len(rows)} rows)", flush=True)
        written.append(destination)
    return written


def fetch_query(client, query):
    result = client.query(query).result(timeout=QUERY_TIMEOUT_SECONDS)
    field_names = [field.name for field in result.schema]
    rows = [dict(row.items()) for row in result]
    return field_names, rows


def _trim_number(text):
    if "." not in text:
        return text
    trimmed = text.rstrip("0").rstrip(".")
    if trimmed in ("", "-"):
        return "0"
    return trimmed


def main():
    dump_sample_data()


if __name__ == "__main__":
    main()
