#!/usr/bin/env python3

import csv
import os
import re
import argparse
from collections import defaultdict
import logging
from datetime import datetime
import sqlite3
import pandas as pd

SQLITE_SCHEMA = {
    "table_name": "s3_paths",
    "columns": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "path": "TEXT NOT NULL UNIQUE",
        "year": "INTEGER NOT NULL",
        "month": "INTEGER NOT NULL",
        "day": "INTEGER NOT NULL",
        "origin": "TEXT NOT NULL",
        "bucket": "TEXT NOT NULL",
        "archived": "INTEGER NOT NULL",
        "deleted": "INTEGER NOT NULL",
        "destination_path": "TEXT NOT NULL",
        "size": "INTEGER NOT NULL",
        "date": "TEXT NOT NULL",
    },
}


def setup_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    columns_def = ", ".join(
        [f"{col} {dtype}" for col, dtype in SQLITE_SCHEMA["columns"].items()]
    )
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {SQLITE_SCHEMA['table_name']} ({columns_def})"
    )

    conn.commit()
    return conn, cursor


def setup_indexes(db: sqlite3.Connection) -> None:
    """
    Create necessary indexes to improve query performance.
    """
    cursor = db.cursor()

    # Index on destination_path (most critical as it's used in multiple queries)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_destination_path ON s3_paths(destination_path)"
    )

    # Composite index on archived and deleted
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_archived_deleted ON s3_paths(archived, deleted)"
    )

    # Composite index on year, month, day
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_date_parts ON s3_paths(year, month, day)"
    )

    # Index on path (since it's used in the UNIQUE constraint)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_path ON s3_paths(path)")

    db.commit()
    logging.info("Database indexes created/verified")


def setup_logging(file_path=None):
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if file_path is None:
        log_file = os.path.join(log_dir, f"csv_filter_{timestamp}.log")
    else:
        log_file = file_path

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def normalize_path(filepath):
    return filepath.replace("%3D", "=")


def extract_date_parts(filepath):
    pattern = r"year(?:%3D|=)(\d+)/month(?:%3D|=)(\d+)/day(?:%3D|=)(\d+)"
    match = re.search(pattern, filepath)

    if match:
        year, month, day = match.groups()
        return year, month, day

    return "unknown", "unknown", "unknown"


def extract_origin(filepath):
    raw_pos = filepath.find("raw/")
    if raw_pos == -1:
        return "unknown"
    parts = filepath[raw_pos:].split("/")
    for i, part in enumerate(parts):
        if part.startswith("year%3D") or part.startswith("year="):
            if i > 1:
                return "/".join(parts[: i - 1])
            break

    return "unknown"


def process_s3_csv(
    input_file, path_column_idx=1, chunk_size=100000, conn=None, cursor=None
):
    logging.info(f"Starting to process {input_file}")
    start_time = datetime.now()

    reader = pd.read_csv(
        input_file,
        chunksize=chunk_size,
        skip_blank_lines=True,
        header=None,
    )

    for chunk in reader:
        for _, row in chunk.iterrows():
            bucket = row.iloc[0]
            path = row.iloc[path_column_idx]
            normalized_path = normalize_path(path)
            normalized_row = row.copy()
            normalized_row.iloc[path_column_idx] = normalized_path
            date_parts = extract_date_parts(normalized_path)
            origin = extract_origin(normalized_path)
            destination_path = (
                f"archive/{origin}/{date_parts[0]}-{date_parts[1]}-{date_parts[2]}.tar"
            )

            year, month, day = date_parts
            size = normalized_row.iloc[2]
            date = normalized_row.iloc[3]

            try:
                logging.info(
                    f"Inserting {normalized_path} {year} {month} {day} {origin} {bucket} {destination_path} {size} {date}"
                )
                cursor.execute(
                    "INSERT INTO s3_paths (path, year, month, day, origin, bucket, archived, deleted, destination_path, size, date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        normalized_path,
                        year,
                        month,
                        day,
                        origin,
                        bucket,
                        0,
                        0,
                        destination_path,
                        size,
                        date,
                    ),
                )

            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed" in str(e):
                    continue
                else:
                    raise
        conn.commit()

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"Time taken: {duration:.2f} seconds")


def main():
    parser = argparse.ArgumentParser(
        description="Filter a large CSV file with S3 paths into files based on origin and date"
    )
    parser.add_argument("input_files", nargs="+", help="Input CSV files paths")
    parser.add_argument(
        "--output-dir",
        "-o",
        default="output",
        help="Output directory for filtered files",
    )
    parser.add_argument(
        "--path-column",
        "-p",
        type=int,
        default=1,
        help="Column index (0-based) containing the S3 path",
    )
    parser.add_argument(
        "--chunk-size",
        "-c",
        type=int,
        default=100000,
        help="Number of rows to process in each chunk",
    )

    args = parser.parse_args()

    setup_logging()
    conn, cursor = setup_sqlite("s3_paths.db")

    # Create indexes to improve performance
    setup_indexes(conn)

    # Process all input files
    for input_file in args.input_files:
        process_s3_csv(
            input_file,
            path_column_idx=args.path_column,
            chunk_size=args.chunk_size,
            conn=conn,
            cursor=cursor,
        )

        print(f"Processed {input_file}")
        print("\n")


if __name__ == "__main__":
    main()
