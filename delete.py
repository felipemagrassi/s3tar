import time
import pandas as pd
import boto3
import threading
import glob
import os
from datetime import datetime
chunk_size = 10000

df = pd.read_csv('a.csv', chunksize=chunk_size, header=None)

def log_info(message):
    print(f'{datetime.now()}: {message}')

def fix_paths(paths):
    return [path.replace('%3D', '=') for path in paths]

def delete_objects(bucket, paths, batch_number):
    s3 = boto3.client('s3')
    result = s3.delete_objects(Bucket=bucket, 
                    Delete={'Objects': [{'Key': path} for path in paths], 'Quiet': False})
    
    # Create results directory if it doesn't exist
    os.makedirs('results', exist_ok=True)
    
    with open(f'results/deletion_results_{batch_number}.csv', 'a') as f:
        for deleted in result.get("Deleted", []):
            f.write(f"{bucket},{deleted['Key']},deleted\n")
        for error in result.get("Errors", []):
            f.write(f"{bucket},{error['Key']},error\n")

batch_number = 0
for chunk in df:
    bucket = 's3tar'
    paths = chunk.iloc[:, 1].tolist()  
    log_info(f'Deleting batch {batch_number} of {len(paths)} objects')
    fixed_paths = fix_paths(paths)
    thread_chunk_size = 1000
    thread_chunks = [fixed_paths[i:i + thread_chunk_size] for i in range(0, len(fixed_paths), thread_chunk_size)]
    threads = []
    for i, thread_chunk in enumerate(thread_chunks):
        thread = threading.Thread(
            target=delete_objects,
            args=(bucket, thread_chunk, f"{batch_number}-{i}")
        )
        threads.append(thread)
        thread.start()
        log_info(f'Started thread {i} for batch {batch_number} with {len(thread_chunk)} objects')
    for i, thread in enumerate(threads):
        thread.join()
        log_info(f'Thread {i} for batch {batch_number} completed')
    batch_number += 1

    
log_info('Joining all CSV files into a single results file')
result_files = glob.glob('results/deletion_results_*.csv')
with open('results/deletion_results.csv', 'w') as outfile:
    for filename in result_files:
        with open(filename, 'r') as infile:
            outfile.write(infile.read())
    
log_info(f'Successfully joined {len(result_files)} CSV files into results/deletion_results.csv')
