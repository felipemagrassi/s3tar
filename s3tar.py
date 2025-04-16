#!/usr/bin/env python3

import os
import json
import boto3
import argparse
import concurrent.futures
import subprocess
import logging
from datetime import datetime, timedelta
import re
from collections import defaultdict
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global configuration
STATE_FILE = "s3tar_state.json"
COMPANY = "a"

def load_state():
    """Load the current processing state from file"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                logger.info(f"Loaded state file: {len(state.get('processed_paths', []))} paths already processed")
                return state
        except Exception as e:
            logger.warning(f"Error loading state file: {str(e)}")
    return {"processed_paths": [], "last_prefix": None, "continuation_token": None}

def save_state(state):
    """Save the current processing state to file"""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
        logger.info(f"Saved state: {len(state.get('processed_paths', []))} paths processed")
    except Exception as e:
        logger.error(f"Error saving state file: {str(e)}")

def get_s3_client(region, profile=None):
    """Create and return an S3 client"""
    session_args = {}
    if profile:
        session_args['profile_name'] = profile
    
    session = boto3.Session(**session_args)
    return session.client('s3', region_name=region)

def is_valid_path(path_info):
    """
    Check if a path should be processed based on business rules
    
    Args:
        path_info (dict): Path information containing year, month, day
        
    Returns:
        bool: True if path should be processed, False otherwise
    """
    year = path_info.get('year')
    month = path_info.get('month')
    day = path_info.get('day')
    
    if not all([year, month, day]):
        return False
    
    # Ignore files from day 1
    if day == 1:
        logger.debug(f"Ignoring path with day=01: {path_info['path']}")
        return False
    
    # Ignore files from 3 months to today
    today = datetime.now()
    file_date = datetime(year, month, day)
    three_months_ago = today - timedelta(days=90)
    
    if file_date > three_months_ago:
        logger.debug(f"Ignoring path with date too recent (within 3 months): {path_info['path']}")
        return False
    
    return True

def extract_date_info(path):
    """
    Extract date information from a path
    
    Args:
        path (str): S3 path
        
    Returns:
        dict: Dictionary with year, month, day, and full path
    """
    year_match = re.search(r'year=(\d{4})', path)
    month_match = re.search(r'month=(\d{2})', path)
    day_match = re.search(r'day=(\d{2})', path)
    
    if not all([year_match, month_match, day_match]):
        return None
    
    return {
        'year': int(year_match.group(1)),
        'month': int(month_match.group(1)),
        'day': int(day_match.group(1)),
        'path': path
    }

def find_paths(bucket, region, prefix=None, max_paths=100, profile=None):
    """
    Find paths to process in S3
    
    Args:
        bucket (str): S3 bucket name
        region (str): AWS region
        prefix (str, optional): Prefix filter to limit search scope
        max_paths (int): Maximum number of paths to return
        profile (str, optional): AWS profile name
        
    Returns:
        dict: Hierarchical structure of paths and their processing status
    """
    # Initialize S3 client
    s3_client = get_s3_client(region, profile)
    
    # Load previous state
    state = load_state()
    processed_paths = set(state.get('processed_paths', []))
    
    # Get continuation token if available
    continuation_token = state.get('continuation_token')
    last_prefix = state.get('last_prefix')
    
    # If prefix is not specified but we have a last prefix, use it
    if prefix is None and last_prefix:
        prefix = last_prefix
        logger.info(f"Resuming from saved prefix: {prefix}")
    
    # If prefix is still None, find available prefixes
    if prefix is None:
        prefixes = list_top_prefixes(s3_client, bucket, f"raw/{COMPANY}")
        if not prefixes:
            logger.error("No prefixes found to process")
            return {}
        prefix = prefixes[0]
    
    logger.info(f"Finding paths in s3://{bucket}/{prefix}...")
    
    # Initialize path tree structure: {year: {month: {day: {size, processed, path}}}}
    path_tree = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'size': 0, 'processed': False, 'path': ''})))
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pagination_config = {'PageSize': 1000}
    
    pagination_args = {
        'Bucket': bucket,
        'Prefix': prefix,
        'PaginationConfig': pagination_config
    }
    
    if continuation_token:
        pagination_args['StartingToken'] = continuation_token
    
    paths_found = 0
    next_continuation_token = None
    try:
        page_iterator = paginator.paginate(**pagination_args)
        for page in page_iterator:
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                key = obj['Key']
                key_size = obj.get('Size', 0)
                
                # Split the key into parts
                path_parts = key.split('/')
                
                # Skip if path is too short
                if len(path_parts) < 8:  # Need at least raw/company/short/object/year/month/day/file
                    continue
                
                # Find year, month, day parts
                year_idx = month_idx = day_idx = -1
                for i, part in enumerate(path_parts):
                    if part.startswith('year='):
                        year_idx = i
                    elif part.startswith('month='):
                        month_idx = i
                    elif part.startswith('day='):
                        day_idx = i
                
                # Skip if we don't have all required components
                if -1 in (year_idx, month_idx, day_idx) or max(year_idx, month_idx, day_idx) >= len(path_parts):
                    continue
                
                try:
                    year = int(path_parts[year_idx].split('=')[1])
                    month = int(path_parts[month_idx].split('=')[1])
                    day = int(path_parts[day_idx].split('=')[1])
                except (ValueError, IndexError):
                    continue

                # Build the path including company, short_company, object_type, year, month, day
                # Format should be: company/short_company/object_type/year=YYYY/month=MM/day=DD
                raw_idx = -1
                company_idx = -1
                short_company_idx = -1
                object_type_idx = -1
                
                # Find key indices in the path parts
                for i, part in enumerate(path_parts):
                    if part == 'raw':
                        raw_idx = i
                    elif i == raw_idx + 1:  # Company follows raw
                        company_idx = i
                    elif i == company_idx + 1:  # Short company follows company
                        short_company_idx = i
                    elif i == short_company_idx + 1:  # Object type follows short company
                        object_type_idx = i
                
                # Skip if any required part is missing
                if -1 in (raw_idx, company_idx, short_company_idx, object_type_idx):
                    continue
                
                # Build path from company to day
                day_path_parts = path_parts[company_idx:day_idx+1]
                day_path = '/'.join(day_path_parts)
                
                # Check if this day path is already processed
                if day_path in processed_paths:
                    path_tree[year][month][day]['processed'] = True
                    continue
                
                # Update path information in the tree
                path_tree[year][month][day]['size'] += key_size
                path_tree[year][month][day]['path'] = day_path
                path_tree[year][month][day]['processed'] = False
            
            # Save continuation token for next time
            if 'NextContinuationToken' in page:
                next_continuation_token = page['NextContinuationToken']
            
            # Count unprocessed paths
            paths_found = sum(1 for y in path_tree.values() 
                            for m in y.values() 
                            for d, info in m.items() 
                            if not info['processed'])
            
            if paths_found >= max_paths:
                logger.info(f"Found {paths_found} unprocessed paths, stopping search")
                break
    
    except Exception as e:
        logger.error(f"Error listing objects: {str(e)}")
    
    # Update state with new continuation token
    state['continuation_token'] = next_continuation_token
    state['last_prefix'] = prefix
    save_state(state)
    
    # Log statistics
    total_days = sum(1 for y in path_tree.values() for m in y.values() for d in m.keys())
    processed_days = sum(1 for y in path_tree.values() 
                         for m in y.values() 
                         for info in m.values() 
                         if info['processed'])
    
    logger.info(f"Found {total_days} total day paths ({processed_days} already processed)")
    
    return path_tree

def list_top_prefixes(s3_client, bucket, root_prefix):
    """
    List top-level prefixes in an S3 bucket
    
    Args:
        s3_client: Boto3 S3 client
        bucket (str): S3 bucket name
        root_prefix (str): Root prefix to start from
        
    Returns:
        list: List of prefix strings
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=root_prefix,
            Delimiter='/'
        )
        
        prefixes = []
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                prefixes.append(prefix['Prefix'])
                
        logger.info(f"Found {len(prefixes)} top-level prefixes")
        
        # If no prefixes found, try using the root_prefix itself
        if not prefixes:
            prefixes = [root_prefix]
            
        return prefixes
        
    except Exception as e:
        logger.error(f"Error listing prefixes: {str(e)}")
        return [root_prefix]  # Return root_prefix as fallback

def get_valid_paths(path_tree, max_paths=100):
    """
    Extract valid paths from the path tree
    
    Args:
        path_tree (dict): Hierarchical structure of paths
        max_paths (int): Maximum number of paths to return
        
    Returns:
        list: List of valid path strings to process
    """
    valid_paths = []
    
    for year, months in path_tree.items():
        for month, days in months.items():
            for day, info in days.items():
                if info['processed'] or info['size'] == 0:
                    continue
                    
                path_info = {
                    'year': year,
                    'month': month,
                    'day': day,
                    'path': info['path']
                }
                
                if is_valid_path(path_info):
                    valid_paths.append(info['path'])
                    
                if len(valid_paths) >= max_paths:
                    break
            
            if len(valid_paths) >= max_paths:
                break
                
        if len(valid_paths) >= max_paths:
            break
    
    logger.info(f"Found {len(valid_paths)} valid paths to process")
    return valid_paths

def process_paths(valid_paths, bucket, region, profile=None, max_workers=4):
    """
    Process a list of paths using s3tar
    
    Args:
        valid_paths (list): List of paths to process
        bucket (str): S3 bucket name
        region (str): AWS region
        profile (str, optional): AWS profile name
        max_workers (int): Maximum number of concurrent worker threads
        
    Returns:
        list: List of successfully processed paths
    """
    if not valid_paths:
        logger.info("No paths to process")
        return []
    
    root_prefix = "raw"
    archive_root = "archive"
    
    logger.info(f"Processing {len(valid_paths)} paths with {max_workers} workers")
    
    processed_paths = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create task dictionary
        futures = {}
        for path in valid_paths:
            future = executor.submit(
                archive_single_path, 
                bucket=bucket,
                region=region,
                root_prefix=root_prefix,
                archive_root=archive_root,
                path=path,
                profile=profile
            )
            futures[future] = path
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            path = futures[future]
            try:
                if future.result():
                    processed_paths.append(path)
                    
                    # Update state after each successful archive
                    state = load_state()
                    if path not in state.get('processed_paths', []):
                        state['processed_paths'].append(path)
                        save_state(state)
            except Exception as e:
                logger.error(f"Error processing path {path}: {str(e)}")
    
    logger.info(f"Successfully processed {len(processed_paths)} out of {len(valid_paths)} paths")
    return processed_paths

def archive_single_path(bucket, region, root_prefix, archive_root, path, profile=None):
    """
    Archive a single path using s3tar
    
    Args:
        bucket (str): S3 bucket name
        region (str): AWS region
        root_prefix (str): Root prefix for source objects
        archive_root (str): Root prefix for archived objects
        path (str): Path to archive
        profile (str, optional): AWS profile name
        
    Returns:
        bool: True if successful, False otherwise
    """
    s3_client = get_s3_client(region, profile)
    
    src = f"s3://{bucket}/{root_prefix}/{path}/"
    dst = f"s3://{bucket}/{archive_root}/{root_prefix}/{path}.tar"
    
    logger.info(f"🔎 Checking objects in {src}...")
    
    # Count objects
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{root_prefix}/{path}/"
        )
        
        object_count = len(response.get('Contents', []))
        
        if object_count < 2:
            logger.warning(f"⚠️ Ignoring {src} (not enough files: {object_count})")
            return False
    except Exception as e:
        logger.error(f"Error counting objects: {str(e)}")
        return False
    
    logger.info(f"🗜️ Archiving {src} → {dst}...")
    
    # Build the s3tar command
    cmd = [
        "s3tar",
        "--region", region,
        "-vv",
        "-c",
        "-f", dst,
        "--concat-in-memory",
        "--storage-class", "DEEP_ARCHIVE"
    ]
    
    if profile:
        cmd.extend(["--profile", profile])
    
    cmd.append(src)
    
    # Execute s3tar command
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"✅ Successfully archived {src}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to archive {src}: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        return False

def delete_paths(paths_to_delete, bucket, region, profile=None, max_workers=4):
    """
    Delete original files after successful archiving
    
    Args:
        paths_to_delete (list): List of paths to delete
        bucket (str): S3 bucket name
        region (str): AWS region
        profile (str, optional): AWS profile name
        max_workers (int): Maximum number of concurrent worker threads
        
    Returns:
        list: List of successfully deleted paths
    """
    if not paths_to_delete:
        logger.info("No paths to delete")
        return []
    
    root_prefix = "raw"
    
    logger.info(f"Deleting {len(paths_to_delete)} paths with {max_workers} workers")
    
    deleted_paths = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create task dictionary
        futures = {}
        for path in paths_to_delete:
            future = executor.submit(
                delete_single_path, 
                bucket=bucket,
                region=region,
                root_prefix=root_prefix,
                path=path,
                profile=profile
            )
            futures[future] = path
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            path = futures[future]
            try:
                if future.result():
                    deleted_paths.append(path)
            except Exception as e:
                logger.error(f"Error deleting path {path}: {str(e)}")
    
    logger.info(f"Successfully deleted {len(deleted_paths)} out of {len(paths_to_delete)} paths")
    return deleted_paths

def delete_single_path(bucket, region, root_prefix, path, profile=None):
    """
    Delete a single path
    
    Args:
        bucket (str): S3 bucket name
        region (str): AWS region
        root_prefix (str): Root prefix for source objects
        path (str): Path to delete
        profile (str, optional): AWS profile name
        
    Returns:
        bool: True if successful, False otherwise
    """
    src = f"s3://{bucket}/{root_prefix}/{path}/"
    
    logger.info(f"🧹 Deleting original files from {src}...")
    
    # Build the aws s3 rm command
    cmd = ["aws"]
    
    if profile:
        cmd.extend(["--profile", profile])
    
    cmd.extend([
        "s3", "rm", src, "--recursive"
    ])
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"✅ Successfully deleted files from {src}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to delete files from {src}: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Archive S3 objects using s3tar with multi-threading")
    parser.add_argument("--bucket", default="s3tar", help="S3 bucket name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--profile", help="AWS profile name")
    parser.add_argument("--max-workers", type=int, default=4, help="Maximum number of worker threads")
    parser.add_argument("--delete", action="store_true", help="Delete original files after archiving")
    parser.add_argument("--dry-run", action="store_true", help="Only list paths that would be processed without archiving")
    parser.add_argument("--prefix", help="Process only paths with this prefix")
    parser.add_argument("--max-paths", type=int, default=100, help="Maximum number of paths to process per chunk")
    parser.add_argument("--clear-state", action="store_true", help="Clear saved state before starting")
    
    args = parser.parse_args()
    
    if args.clear_state and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        logger.info("Cleared previous state file")
    
    start_time = time.time()
    
    path_tree = find_paths(
        bucket=args.bucket,
        region=args.region,
        prefix=args.prefix,
        max_paths=args.max_paths,
        profile=args.profile
    )
    
    valid_paths = get_valid_paths(path_tree, args.max_paths)
    
    if args.dry_run:
        print("\nPaths that would be processed:")
        root_prefix = "raw"
        for path in valid_paths:
            print(f"s3://{args.bucket}/{root_prefix}/{path}/")
        print(f"\nTotal: {len(valid_paths)} paths")
    else:
        processed_paths = process_paths(
            valid_paths=valid_paths,
            bucket=args.bucket,
            region=args.region,
            profile=args.profile,
            max_workers=args.max_workers
        )
        
        # Delete original files if requested
        if args.delete and processed_paths:
            deleted_paths = delete_paths(
                paths_to_delete=processed_paths,
                bucket=args.bucket,
                region=args.region,
                profile=args.profile,
                max_workers=args.max_workers
            )
    
    # Log elapsed time
    elapsed_time = time.time() - start_time
    logger.info(f"Job completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()
