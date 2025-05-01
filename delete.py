import time
import pandas as pd
import boto3
import threading
import glob
import os
import json
import sys
from datetime import datetime

# Configuration
CHUNK_SIZE = 10000
THREAD_CHUNK_SIZE = 1000
CSV_FILE = 'a.csv'
CHECKPOINT_FILE = 'checkpoint.json'
RESULTS_DIR = 'results'

def log_info(message):
    """Log a message with timestamp"""
    print(f'{datetime.now()}: {message}')

def fix_paths(paths):
    """Replace URL-encoded characters in S3 paths"""
    return [path.replace('%3D', '=') for path in paths]

def delete_objects(bucket, paths, batch_number):
    """Delete a batch of objects from S3 and log results"""
    try:
        s3 = boto3.client('s3')
        result = s3.delete_objects(
            Bucket=bucket, 
            Delete={'Objects': [{'Key': path} for path in paths], 'Quiet': False}
        )
        
        # Create results directory if it doesn't exist
        os.makedirs(RESULTS_DIR, exist_ok=True)
        
        with open(f'{RESULTS_DIR}/deletion_results_{batch_number}.csv', 'a') as f:
            for deleted in result.get("Deleted", []):
                f.write(f"{bucket},{deleted['Key']},deleted\n")
            for error in result.get("Errors", []):
                f.write(f"{bucket},{error['Key']},error,{error.get('Message', 'Unknown error')}\n")
                
        log_info(f'Batch {batch_number}: Deleted: {len(result.get("Deleted", []))}, Errors: {len(result.get("Errors", []))}')
    except Exception as e:
        log_info(f'Error in batch {batch_number}: {str(e)}')
        # Write all paths as errors
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(f'{RESULTS_DIR}/deletion_results_{batch_number}.csv', 'a') as f:
            for path in paths:
                f.write(f"{bucket},{path},error,{str(e)}\n")

def save_checkpoint(batch_number):
    """Save the current batch number to checkpoint file"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({'last_completed_batch': batch_number - 1}, f)
    log_info(f'Checkpoint saved: completed up to batch {batch_number - 1}')

def load_checkpoint():
    """Load the last completed batch number from checkpoint file"""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
                return checkpoint.get('last_completed_batch', -1)
    except Exception as e:
        log_info(f'Error loading checkpoint: {str(e)}')
    return -1

def combine_result_files():
    """Combine all result files into a single file"""
    log_info('Joining all CSV files into a single results file')
    result_files = glob.glob(f'{RESULTS_DIR}/deletion_results_*.csv')
    
    with open(f'{RESULTS_DIR}/deletion_results.csv', 'w') as outfile:
        # Write header
        outfile.write("bucket,key,status,message\n")
        
        for filename in result_files:
            with open(filename, 'r') as infile:
                outfile.write(infile.read())
    
    log_info(f'Successfully joined {len(result_files)} CSV files into {RESULTS_DIR}/deletion_results.csv')

def main():
    # Create results directory if it doesn't exist
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE):
        log_info(f'Error: CSV file {CSV_FILE} not found')
        sys.exit(1)
    
    # Load the last processed batch from checkpoint
    last_batch = load_checkpoint()
    log_info(f'Starting from batch {last_batch + 1}')
    
    # Read CSV in chunks
    df_chunks = pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE, header=None)
    
    batch_number = 0
    for chunk in df_chunks:
        # Skip already processed batches
        if batch_number <= last_batch:
            batch_number += 1
            log_info(f'Skipping already processed batch {batch_number - 1}')
            continue
        
        try:
            # Extract bucket and paths from the CSV
            # First column is bucket, second column is path
            rows = chunk.values.tolist()
            bucket_path_pairs = [(row[0].strip('"'), row[1].strip('"')) for row in rows]
            
            # Group paths by bucket
            bucket_to_paths = {}
            for bucket, path in bucket_path_pairs:
                if bucket not in bucket_to_paths:
                    bucket_to_paths[bucket] = []
                bucket_to_paths[bucket].append(path)
            
            # Process each bucket separately
            for bucket, paths in bucket_to_paths.items():
                log_info(f'Processing batch {batch_number} for bucket {bucket} with {len(paths)} objects')
                
                # Fix paths
                fixed_paths = fix_paths(paths)
                
                # Split paths into chunks for threading
                thread_chunks = [fixed_paths[i:i + THREAD_CHUNK_SIZE] 
                                for i in range(0, len(fixed_paths), THREAD_CHUNK_SIZE)]
                
                # Create and start threads
                threads = []
                for i, thread_chunk in enumerate(thread_chunks):
                    thread = threading.Thread(
                        target=delete_objects,
                        args=(bucket, thread_chunk, f"{batch_number}-{i}")
                    )
                    threads.append(thread)
                    thread.start()
                    log_info(f'Started thread {i} for batch {batch_number} with {len(thread_chunk)} objects')
                
                # Wait for all threads to complete
                for i, thread in enumerate(threads):
                    thread.join()
                    log_info(f'Thread {i} for batch {batch_number} completed')
            
            # Save checkpoint after each batch
            save_checkpoint(batch_number + 1)
            
        except KeyboardInterrupt:
            log_info('Process interrupted by user. Progress has been saved.')
            combine_result_files()
            sys.exit(0)
        except Exception as e:
            log_info(f'Error processing batch {batch_number}: {str(e)}')
            # Continue to the next batch
        
        batch_number += 1
    
    # Combine all result files
    combine_result_files()
    log_info('All batches processed successfully')

if __name__ == "__main__":
    main()
