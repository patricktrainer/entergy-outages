"""
Bulk extraction of historical outage data from git history into DuckDB.

Two-phase pipeline:
  1. Git → Parquet: iterate commits, parse JSON, write batches to Parquet files
  2. Parquet → DuckDB: bulk load via read_parquet() with dedup

Usage:
    python entergy_outages/build_duckdb.py [--db entergy_outages.duckdb] [--repo .]
"""

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

import duckdb
import git
import pyarrow as pa
import pyarrow.parquet as pq


COUNTY_FILE = "entergy_outages_county.json"
ZIPCODE_FILE = "entergy_outages_zipcode.json"

COUNTY_SCHEMA = pa.schema([
    ("county", pa.string()),
    ("state", pa.string()),
    ("customers_served", pa.int32()),
    ("customers_affected", pa.int32()),
    ("percentage_with_power", pa.float64()),
    ("last_updated_time", pa.string()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("loaded_at", pa.string()),
    ("commit_hash", pa.string()),
    ("commit_time", pa.string()),
])

ZIPCODE_SCHEMA = pa.schema([
    ("zip", pa.string()),
    ("state", pa.string()),
    ("customers_served", pa.int32()),
    ("customers_affected", pa.int32()),
    ("last_updated_time", pa.string()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("loaded_at", pa.string()),
    ("commit_hash", pa.string()),
    ("commit_time", pa.string()),
])

BATCH_SIZE = 500_000
ROW_GROUP_SIZE = 122_880


def iterate_file_versions(repo: git.Repo, filepath: str, ref: str = "main"):
    """Iterate all versions of a file using GitPython's CmdObjectDB."""
    commits = repo.iter_commits(ref, paths=filepath)
    for commit in commits:
        try:
            blob = commit.tree / filepath
            yield commit.committed_datetime.isoformat(), commit.hexsha, blob.data_stream.read()
        except (KeyError, TypeError):
            continue


def parse_county_rows(data: list[dict], commit_hash: str, commit_time: str) -> list[tuple]:
    rows = []
    for item in data:
        rows.append((
            item.get("county"),
            item.get("state"),
            item.get("customersServed"),
            item.get("customersAffected"),
            item.get("percentageWithPower"),
            item.get("lastUpdatedTime"),
            item.get("latitude"),
            item.get("longitude"),
            item.get("_loaded_at"),
            commit_hash,
            commit_time,
        ))
    return rows


def parse_zipcode_rows(data: list[dict], commit_hash: str, commit_time: str) -> list[tuple]:
    rows = []
    for item in data:
        rows.append((
            item.get("zip"),
            item.get("state"),
            item.get("customersServed"),
            item.get("customersAffected"),
            item.get("lastUpdatedTime"),
            item.get("latitude"),
            item.get("longitude"),
            item.get("_loaded_at"),
            commit_hash,
            commit_time,
        ))
    return rows


def flush_parquet(batch: dict, schema: pa.Schema, output_dir: str, file_idx: int) -> int:
    """Write a batch of columnar data to a Parquet file. Returns new file index."""
    table = pa.table(batch, schema=schema)
    path = os.path.join(output_dir, f"batch_{file_idx:04d}.parquet")
    pq.write_table(table, path, row_group_size=ROW_GROUP_SIZE, compression="snappy")
    return file_idx + 1


def process_file_to_parquet(
    repo: git.Repo,
    filepath: str,
    parse_fn,
    schema: pa.Schema,
    output_dir: str,
) -> tuple[int, int, int]:
    """Extract all git versions of a file into Parquet files.

    Returns (commit_count, row_count, file_count).
    """
    print(f"\nPhase 1: Extracting {filepath} → Parquet...")

    columns = schema.names
    batch = {col: [] for col in columns}
    file_idx = 0
    row_count = 0
    commit_count = 0
    skipped = 0
    start = time.time()

    for commit_time, commit_hash, file_bytes in iterate_file_versions(repo, filepath):
        commit_count += 1

        try:
            data = json.loads(file_bytes)
        except (json.JSONDecodeError, ValueError):
            skipped += 1
            continue

        rows = parse_fn(data, commit_hash, commit_time)
        for row in rows:
            for i, col in enumerate(columns):
                batch[col].append(row[i])
        row_count += len(rows)

        if len(batch[columns[0]]) >= BATCH_SIZE:
            file_idx = flush_parquet(batch, schema, output_dir, file_idx)
            batch = {col: [] for col in columns}

        if commit_count % 10_000 == 0:
            elapsed = time.time() - start
            rate = commit_count / elapsed if elapsed > 0 else 0
            print(
                f"  [{commit_count:,}] {row_count:,} rows extracted, "
                f"{file_idx} parquet files, {rate:.0f} commits/sec"
            )

    # Flush remaining rows
    if batch[columns[0]]:
        file_idx = flush_parquet(batch, schema, output_dir, file_idx)

    elapsed = time.time() - start
    print(
        f"  Done: {commit_count:,} commits, {row_count:,} rows → "
        f"{file_idx} parquet files in {elapsed:.1f}s"
    )
    if skipped:
        print(f"  ({skipped} commits skipped due to parse errors)")

    return commit_count, row_count, file_idx


COUNTY_SELECT = """
    SELECT
        county, state, customers_served, customers_affected,
        percentage_with_power,
        TRY_CAST(last_updated_time AS TIMESTAMP) AS last_updated_time,
        latitude, longitude,
        TRY_CAST(loaded_at AS TIMESTAMP) AS loaded_at,
        commit_hash,
        TRY_CAST(commit_time AS TIMESTAMPTZ) AS commit_time
    FROM read_parquet('{glob_path}')
    WHERE TRY_CAST(loaded_at AS TIMESTAMP) IS NOT NULL
"""

ZIPCODE_SELECT = """
    SELECT
        zip, state, customers_served, customers_affected,
        TRY_CAST(last_updated_time AS TIMESTAMP) AS last_updated_time,
        latitude, longitude,
        TRY_CAST(loaded_at AS TIMESTAMP) AS loaded_at,
        commit_hash,
        TRY_CAST(commit_time AS TIMESTAMPTZ) AS commit_time
    FROM read_parquet('{glob_path}')
    WHERE TRY_CAST(loaded_at AS TIMESTAMP) IS NOT NULL
"""


def load_parquet_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    parquet_dir: str,
    table_name: str,
    select_sql: str,
    dedup_columns: list[str],
):
    """Load Parquet files into a DuckDB table with dedup, no constraints."""
    glob_path = os.path.join(parquet_dir, "*.parquet")
    dedup_cols = ", ".join(dedup_columns)

    print(f"\nPhase 2: Loading Parquet → {table_name}...")
    start = time.time()

    query = select_sql.format(glob_path=glob_path)
    con.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT DISTINCT ON ({dedup_cols}) *
        FROM ({query})
        ORDER BY {dedup_cols}
    """)

    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    elapsed = time.time() - start
    print(f"  Loaded {count:,} rows into {table_name} in {elapsed:.1f}s")

    return count


def main():
    parser = argparse.ArgumentParser(description="Build DuckDB from git history")
    parser.add_argument("--db", default="entergy_outages.duckdb", help="DuckDB file path")
    parser.add_argument("--repo", default=".", help="Git repo path")
    args = parser.parse_args()

    repo_path = str(Path(args.repo).resolve())
    db_path = args.db

    print(f"Building DuckDB: {db_path}")
    print(f"Git repo: {repo_path}")

    repo = git.Repo(repo_path, odbt=git.GitCmdObjectDB)

    with tempfile.TemporaryDirectory() as tmpdir:
        county_dir = os.path.join(tmpdir, "county")
        zip_dir = os.path.join(tmpdir, "zipcode")
        os.makedirs(county_dir)
        os.makedirs(zip_dir)

        # Phase 1: Git → Parquet
        process_file_to_parquet(
            repo, COUNTY_FILE, parse_county_rows, COUNTY_SCHEMA, county_dir
        )
        process_file_to_parquet(
            repo, ZIPCODE_FILE, parse_zipcode_rows, ZIPCODE_SCHEMA, zip_dir
        )

        # Phase 2: Parquet → DuckDB (no indexes/constraints per DuckDB best practices)
        if Path(db_path).exists():
            Path(db_path).unlink()

        con = duckdb.connect(db_path)

        county_count = load_parquet_to_duckdb(
            con, county_dir, "county_snapshots",
            COUNTY_SELECT, dedup_columns=["county", "loaded_at"],
        )
        zip_count = load_parquet_to_duckdb(
            con, zip_dir, "zipcode_snapshots",
            ZIPCODE_SELECT, dedup_columns=["zip", "loaded_at"],
        )

        print(f"\nDone!")
        print(f"  County snapshots: {county_count:,} rows")
        print(f"  Zipcode snapshots: {zip_count:,} rows")

        con.close()


if __name__ == "__main__":
    main()
