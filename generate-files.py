#!/usr/bin/env python3
import csv
import os
import re
import argparse
from collections import defaultdict
import logging
from datetime import datetime


def setup_logging():
    """Set up logging configuration"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"csv_filter_{timestamp}.log")

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


def extract_company(filepath):
    """
    Extract the company name from a filepath like:
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
        str: A safe directory name (e.g. 'raw_b_b_short')
    """
    return company_path.replace("/", "_")


def is_within_date_range(year, month, day, max_days_ago=90):
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
    return days_diff <= max_days_ago


def process_s3_csv(
    input_file, output_dir, path_column_idx=1, days_filter=90, batch_size=100000
):
    """
    Process a large CSV file with S3 paths and filter by company type and date
    
    Args:
        input_file: Path to the input CSV file
        output_dir: Directory to store the output files
        path_column_idx: Index of the column containing S3 paths (default is 1, 0-indexed)
        days_filter: Only include files from within this many days (default 90)
        batch_size: Number of rows to process before writing to files
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Track statistics
    total_rows = 0
    filtered_rows = defaultdict(int)
    skipped_day1 = 0
    skipped_new_files = 0
    output_files = {}
    file_handles = {}
    writers = {}
    skipped_non_raw = 0
    normalized_count = 0
    
    logging.info(f"Starting to process {input_file}")
    start_time = datetime.now()

    try:
        with open(input_file, "r", newline="", encoding="utf-8") as csvfile:
            # Try to determine the CSV dialect
            try:
                sample = csvfile.read(8192)
                csvfile.seek(0)
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel  # Fallback to standard CSV format
            
            reader = csv.reader(csvfile, dialect)
            
            batch_data = defaultdict(list)
            
            for row_num, row in enumerate(reader, 1):
                if not row or len(row) <= path_column_idx:
                    continue
                
                total_rows += 1
                filepath = row[path_column_idx]
                
                if not "raw/" in filepath:
                    print(f"Skipping non-raw file: {filepath}")
                    skipped_non_raw += 1
                    continue
                
                # Normalize the path (replace %3D with =)
                if "%3D" in filepath:
                    normalized_path = normalize_path(filepath)
                    normalized_count += 1
                else:
                    normalized_path = filepath
                
                row_copy = list(row)
                row_copy[path_column_idx] = normalized_path
                
                year, month, day = extract_date_parts(normalized_path)
                company = extract_company(normalized_path)
                
                # Skip files where day = 1
                if day == "1":
                    print(f"Skipping day=1: {normalized_path}")
                    skipped_day1 += 1
                    continue
                
                # Skip files newer than specified days
                if is_within_date_range(year, month, day, days_filter):
                    print(f"Skipping new file: {normalized_path}")
                    skipped_new_files += 1
                    continue
                
                # Create a combined key with company and date
                date_key = f"{year}_{month}_{day}"
                combined_key = f"{company}_{date_key}"
                company_dir = get_company_dirname(company)
                company_output_dir = os.path.join(output_dir, company_dir)
                
                # Store data using the combined key
                batch_data[combined_key].append(row_copy)
                filtered_rows[combined_key] += 1

                if row_num % batch_size == 0:
                    # Process each company separately
                    for key in list(batch_data.keys()):
                        key_parts = key.split('_')
                        if len(key_parts) >= 4:  # At least company + year + month + day
                            # Extract company and date parts
                            company_parts = key_parts[:-3]
                            company_path = '_'.join(company_parts)
                            company_dir = get_company_dirname(company_path)
                            company_output_dir = os.path.join(output_dir, company_dir)
                            
                            write_batch(
                                {key: batch_data[key]},
                                company_output_dir,
                                None,
                                output_files,
                                file_handles,
                                writers,
                                write_header=False,
                            )
                    batch_data = defaultdict(list)
                    logging.info(f"Processed {row_num:,} rows...")
                
    except Exception as e:
        logging.error(f"Error processing CSV: {str(e)}")
        raise
    finally:
        # Write any remaining batched data
        if batch_data:
            for key in list(batch_data.keys()):
                key_parts = key.split('_')
                if len(key_parts) >= 4:  # At least company + year + month + day
                    # Extract company and date parts
                    company_parts = key_parts[:-3]
                    company_path = '_'.join(company_parts)
                    company_dir = get_company_dirname(company_path)
                    company_output_dir = os.path.join(output_dir, company_dir)
                    
                    write_batch(
                        {key: batch_data[key]},
                        company_output_dir,
                        None,
                        output_files,
                        file_handles,
                        writers,
                        write_header=False,
                    )
            logging.info(f"Wrote final batch of data")
            
        # Close all open file handles
        for file_handle in file_handles.values():
            file_handle.close()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Print summary
    logging.info(f"CSV processing complete")
    logging.info(f"Total rows processed: {total_rows:,}")
    logging.info(f"Paths normalized (replaced %3D with =): {normalized_count:,}")
    logging.info(f"Rows skipped (day=1): {skipped_day1:,}")
    logging.info(f"Rows skipped (newer than {days_filter} days): {skipped_new_files:,}")
    logging.info(f"Rows skipped (non-raw): {skipped_non_raw:,}")
    logging.info(f"Time taken: {duration:.2f} seconds")
    logging.info(f"Processing speed: {total_rows / duration:.2f} rows/second")
    logging.info("Rows per company/date group:")
    
    for file_key, count in sorted(
        filtered_rows.items(), key=lambda x: x[1], reverse=True
    ):
        logging.info(
            f"  {file_key}: {count:,} rows -> {output_files.get(file_key, 'N/A')}"
        )


def write_batch(
    batch_data,
    output_dir,
    headers,
    output_files,
    file_handles,
    writers,
    write_header=True,
):
    """Write batched data to the appropriate output files"""
    for file_key, rows in batch_data.items():
        if not rows:
            continue

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        if file_key not in file_handles:
            # Extract date part from the combined key (last 3 components)
            key_parts = file_key.split('_')
            if len(key_parts) >= 4:  # company_year_month_day format
                # Get the date part for filename
                date_parts = key_parts[-3:]
                date_string = f"{date_parts[0]}_{date_parts[1]}_{date_parts[2]}"
                
                # Get company name from key but use simplified version for filename
                # to avoid path duplication
                filename = f"{date_string}.csv"
            else:
                # Fallback in case we can't parse
                filename = f"{file_key}.csv"
                
            # Create new output file for this key
            output_file = os.path.join(output_dir, filename)

            # Track if this is a new file
            is_new_file = not os.path.exists(output_file)

            # Open file and create writer
            file_handle = open(output_file, "a", newline="", encoding="utf-8")
            writer = csv.writer(file_handle)

            # Write headers only if needed (and is a new file)
            if write_header and headers and is_new_file:
                writer.writerow(headers)

            # Store references
            output_files[file_key] = output_file
            file_handles[file_key] = file_handle
            writers[file_key] = writer

        # Write all rows for this file key
        writers[file_key].writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Filter a large CSV file with S3 paths into files based on company and date"
    )
    parser.add_argument("input_file", help="Input CSV file path")
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
        "--days-filter",
        "-d",
        type=int,
        default=90,
        help="Only include files from within this many days",
    )
    parser.add_argument(
        "--batch-size", "-b", type=int, default=100000, help="Batch size for processing"
    )

    args = parser.parse_args()

    setup_logging()
    process_s3_csv(
        args.input_file,
        args.output_dir,
        path_column_idx=args.path_column,
        days_filter=args.days_filter,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
