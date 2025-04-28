#!/usr/bin/env python3

import csv
import os
import re
import argparse
from collections import defaultdict
import logging
from datetime import datetime
import pandas as pd


NotificationStatus = {
    "success": "success",
    "failed": "failed",
    "skipped": "skipped",
}

NotificationReason = {
    "path_column_missing": "Path column is missing",
    "path_not_a_raw_path": "Path is not a raw path",
    "path_newer_than_90_days": "Path is newer than 90 days",
    "path_day_1": "Path is day 1",
    "path_invalid": "Path is invalid",
}


def setup_logging(file_path=None):
    """
    Set up logging configuration
    
    Args:
        file_path: Optional custom log file path. If None, a default path will be used.
    """
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
    """
    Replace %3D with = in file paths
    
    Args:
        filepath: The S3 file path
        
    Returns:
        str: The normalized path with %3D replaced with =
    """
    return filepath.replace("%3D", "=")


def extract_date_parts(filepath):
    """
    Extract year, month, and day from a filepath containing patterns like:
    year%3D2024/month%3D1/day%3D1 or year=2024/month=1/day=1

    Returns:
        tuple: (year, month, day) or None if pattern not found
    """
    # Handle both %3D and = formats in the pattern
    pattern = r"year(?:%3D|=)(\d+)/month(?:%3D|=)(\d+)/day(?:%3D|=)(\d+)"
    match = re.search(pattern, filepath)

    if match:
        year, month, day = match.groups()
        return year, month, day

    return "unknown", "unknown", "unknown"


def extract_origin(filepath):
    """
    Extract the origin from a filepath like:
    raw/b/b_short/Contact/year%3D2024/month%3D1/day%3D2/appflow/account_1.txt
    or raw/b/b_short/Contact/year=2024/month=1/day=2/appflow/account_1.txt

    Returns:
        str: The object type (Contact, Lead, Opportunity, etc.)
    """
    raw_pos = filepath.find("raw/")
    if raw_pos == -1:
        return "unknown"
    parts = filepath[raw_pos:].split("/")
    
    # Look for parts before year%3D or year= pattern
    for i, part in enumerate(parts):
        if part.startswith("year%3D") or part.startswith("year="):
            if i > 1:
                return "/".join(parts[:i-1])
            break
    
    return "unknown"


def get_company_dirname(company_path):
    """
    Convert a company path to a safe directory name
    
    Args:
        company_path: The full company path (e.g. 'raw/b/b_short')
        
    Returns:
        str: A safe directory name that preserves the structure (e.g. 'raw.b.b_short')
    """
    # Use dot instead of underscore or dash to avoid confusion with paths containing underscores
    return company_path.replace("/", ".")


def newer_than_90_days(year, month, day):
    """
    Check if the date is within the specified range from today

    Args:
        year, month, day: Date parts as strings
        max_days_ago: Maximum days ago to include

    Returns:
        bool: True if date is within range, False otherwise
    """
    file_date = datetime(int(year), int(month), int(day))
    today = datetime.now()
    days_diff = (today - file_date).days
    return days_diff <= 90

def detect_csv_dialect(file_path):
    """
    Determine the CSV dialect for proper parsing
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        csv.Dialect: Detected dialect or csv.excel as fallback
    """
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as f:
            sample = f.read(8192)
            dialect = csv.Sniffer().sniff(sample)
    except (csv.Error, UnicodeDecodeError):
        dialect = csv.excel  # Fallback to standard CSV format
    
    return dialect

def is_raw_path(path):
    """
    Check if a path is a raw path
    """
    return "raw/" in str(path)

def validate_path(path, statistics, notification):
    """
    Validate a path
    """

    if not is_raw_path(path):
        statistics["skipped_non_raw"] += 1
        notification["status"] = NotificationStatus["failed"]
        notification["reason"] = NotificationReason["path_not_a_raw_path"]
        return False

    year, month, day = extract_date_parts(path)
    if newer_than_90_days(year, month, day):
        statistics["skipped_new_files"] += 1
        notification["status"] = NotificationStatus["failed"]
        notification["reason"] = NotificationReason["path_newer_than_90_days"]
        return False

    if day == "1":
        statistics["skipped_day_1"] += 1
        notification["status"] = NotificationStatus["failed"]
        notification["reason"] = NotificationReason["path_day_1"]
        return False

    return True

def get_dirname_by_origin(origin):
    """
    Get the directory name based on the origin
    """
    return origin.replace("/", ".")

def build_failed_file_path(notification, failed_path):
    """
    Build the file path for the failed file
    """
    reason = None
    if notification["reason"] == NotificationReason["path_column_missing"]:
        reason = "missing"
    elif notification["reason"] == NotificationReason["path_not_a_raw_path"]:
        reason = "not_raw"
    elif notification["reason"] == NotificationReason["path_newer_than_90_days"]:
        reason = "90_days"
    elif notification["reason"] == NotificationReason["path_day_1"]:
        reason = "day_1"
    elif notification["reason"] == NotificationReason["path_invalid"]:
        reason = "invalid"

    return f"{failed_path}_{reason}.csv"

def append_to_file(row, file_path):
    """
    Append a row to a file
    """
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

    try:
        with open(file_path, 'a') as f:
            # Write the row as it appears in the original CSV file
            if isinstance(row, pd.Series):
                # Join the values with commas and add a newline
                line = ','.join([f'"{val}"' if isinstance(val, str) else str(val) for val in row])
                f.write(line + '\n')
            else:
                # If it's already a string (like from file.csv), write it directly
                if isinstance(row, str):
                    f.write(row if row.endswith('\n') else row + '\n')
                else:
                    # Handle case where row is not a string
                    f.write(str(row) + '\n')
    except Exception as e:
        logging.error(f"Error appending to file {file_path}: {e}")
    finally:
        f.close()


def process_s3_csv(
    input_file, 
    output_dir, 
    path_column_idx=1, 
    chunk_size=100000
):
    """
    Process a large CSV file with S3 paths using pandas chunking
    
    Args:
        input_file: Path to the input CSV file
        output_dir: Directory to store the output files
        path_column_idx: Index of the column containing S3 paths (default is 1, 0-indexed)
        chunk_size: Number of rows to process in each chunk
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Track statistics
    statistics = {
        "total_rows": 0,
        "skipped_day_1": 0,
        "skipped_new_files": 0,
        "skipped_non_raw": 0,
        "skipped_invalid_path": 0,
        "skipped_row": 0,
        "normalized_count": 0,
        "filtered_rows": defaultdict(int)
    }
    
    logging.info(f"Starting to process {input_file}")
    start_time = datetime.now()
    
    dialect = detect_csv_dialect(input_file)
    
    reader = pd.read_csv(
        input_file, 
        chunksize=chunk_size,
        dialect=dialect,
        skip_blank_lines=True,
        header=None
    )

    for chunk in reader:
        for _, row in chunk.iterrows():
            notification = {
                "data": row,
                "status": None,
                "file_path": None,
                "reason": None,
            }
            statistics["total_rows"] += 1

            if path_column_idx >= len(row) or pd.isna(row.iloc[path_column_idx]):
                statistics["skipped_row"] += 1
                notification["status"] = NotificationStatus["failed"]
                notification["reason"] = NotificationReason["path_column_missing"]

            normalized_path = normalize_path(row.iloc[path_column_idx])
            normalized_row = row.copy()
            normalized_row.iloc[path_column_idx] = normalized_path
            date_parts = extract_date_parts(normalized_path)
            origin = extract_origin(normalized_path)
            folder_name = get_dirname_by_origin(origin)

            file_name = f"{date_parts[0]}.{date_parts[1]}.{date_parts[2]}.csv"
            full_path = f"{output_dir}/{folder_name}/{file_name}"
            failed_path = f"{output_dir}/failed/{folder_name}/{file_name}".replace(".csv", "")

            statistics["normalized_count"] += 1

            validate_path(normalized_path, statistics, notification)

            if notification["status"] == NotificationStatus["failed"]:
                notification["file_path"] = build_failed_file_path(notification, failed_path)
                append_to_file(normalized_row, notification["file_path"])
                continue

            if origin == "unknown":
                statistics["skipped_invalid_path"] += 1
                notification["status"] = NotificationStatus["failed"]
                notification["reason"] = NotificationReason["path_invalid"]
                notification["file_path"] = build_failed_file_path(notification, failed_path)
                append_to_file(normalized_row, notification["file_path"])
                continue

            append_to_file(normalized_row, full_path)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Print summary
    logging.info(f"CSV processing complete")
    logging.info(f"Total rows processed: {statistics['total_rows']:,}")
    logging.info(f"Paths normalized (replaced %3D with =): {statistics['normalized_count']:,}")

    logging.info(f"Skipped day 1: {statistics['skipped_day_1']:,}")
    logging.info(f"Skipped new files: {statistics['skipped_new_files']:,}") 
    logging.info(f"Skipped non-raw: {statistics['skipped_non_raw']:,}")
    logging.info(f"Skipped invalid path: {statistics['skipped_invalid_path']:,}")
    logging.info(f"Skipped row: {statistics['skipped_row']:,}")

    logging.info(f"Time taken: {duration:.2f} seconds")
    logging.info(f"Processing speed: {statistics['total_rows'] / duration:.2f} rows/second")
    
def main():
    parser = argparse.ArgumentParser(
        description="Filter a large CSV file with S3 paths into files based on origin and date"
    )
    parser.add_argument("input_files", nargs='+', help="Input CSV files paths")
    parser.add_argument(
        "--output-dir",
        "-o",
        default="filtered_output",
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
        help="Number of rows to process in each chunk"
    )

    args = parser.parse_args()

    setup_logging()
    
    # Process all input files
    for input_file in args.input_files:
        process_s3_csv(
            input_file,
            args.output_dir,
            path_column_idx=args.path_column,
            chunk_size=args.chunk_size,
        )

        print(f"Processed {input_file}")
        print("\n")


if __name__ == "__main__":
    main()
