import logging
import boto3
import botocore
import argparse
import hashlib
import os
import json
import subprocess
import csv
import re
from collections import deque
from datetime import datetime


logging.basicConfig(
    level=logging.DEBUG, format="%(message)s", handlers=[logging.StreamHandler()]
)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def log_debug(message: str) -> None:
    logging.debug(f"🔍 DEBUG: {message}")


def log_info(message: str) -> None:
    logging.info(f"ℹ️ INFO: {message}")


def log_success(message: str) -> None:
    logging.info(f"✅ SUCESSO: {message}")


def log_warning(message: str) -> None:
    logging.warning(f"⚠️ AVISO: {message}")


def log_error(message: str) -> None:
    logging.error(f"❌ ERRO: {message}")


def log_dry_run(message: str) -> None:
    logging.info(f"🔬 DRY-RUN: {message}")


def archive_object(
    region: str,
    profile: str,
    bucket: str,
    dst_path: str,
    src_csv_path: str,
    deep_archive: bool,
    dry_run: bool,
) -> bool:
    """
    Archive objects listed in a CSV file into a TAR archive
    
    Args:
        region: AWS region
        profile: AWS profile
        bucket: S3 bucket name
        dst_path: Destination path for TAR archive
        src_csv_path: Path to the CSV file containing objects to archive
        deep_archive: Use DEEP_ARCHIVE storage class
        dry_run: Only simulate, don't execute
        
    Returns:
        bool: True if successful, False otherwise
    """
    full_dst_path = f"s3://{bucket}/{dst_path}"
    
    if dry_run:
        log_dry_run(f"Simulating archiving objects from {src_csv_path} to {full_dst_path}")
        return True

    cmd = [
        "s3tar",
        "--region",
        region,
        "-vvv",
        "-c",
        "-f",
        full_dst_path,
        "--concat-in-memory",
    ]
    
    if deep_archive:
        cmd.append("--storage-class")
        cmd.append("DEEP_ARCHIVE")
        
    if profile:
        cmd.append("--profile")
        cmd.append(profile)
        
    # Use the CSV file as a manifest
    cmd.append("-m")
    cmd.append(src_csv_path)

    try:
        log_info(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        log_success(f"Archived objects from {src_csv_path} to {full_dst_path}")
        return True
    except subprocess.CalledProcessError as e:
        log_error(f"Error archiving objects from {src_csv_path}: {e}")
        return False


def delete_object(
    s3client: boto3.client, bucket: str, src_path: str, delete: bool, dry_run: bool
) -> bool:
    if dry_run:
        log_dry_run(f"Simulando exclusão de {src_path}")
        return True

    if not delete:
        log_warning(f"Deleção nao habilitada, ignorando {src_path}")
        return False

    try:
        objects = []
        paginator = s3client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=src_path)

        for page in page_iterator:
            if "Contents" in page:
                objects.extend([{"Key": obj["Key"]} for obj in page["Contents"]])

        if not objects:
            return True

        chunk_size = 1000
        for i in range(0, len(objects), chunk_size):
            chunk = objects[i : i + chunk_size]
            s3client.delete_objects(Bucket=bucket, Delete={"Objects": chunk})

        log_success(f"Excluido {src_path}")
        return True
    except botocore.exceptions.ClientError as e:
        log_error(f"Erro ao excluir objetos: {e}")
        return False

def process_file(file_path: str) -> str:
    """
    Process a file and return info needed to archive and delete
    raw_b_b_short/2024_1_2.csv

    returning "s3://bucket/raw/b/b_short/2024_1_2.csv",

    """

    folder_path = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)

    print(f"folder_path: {folder_path}")
    print(f"file_name: {file_name}")

    return f"s3://bucket/{folder_path}/{file_name}"


def main():
    ###### Arguments ######
    parser = argparse.ArgumentParser(
        description="Archive S3 objects from inventory files into TAR archives"
    )
    parser.add_argument("--bucket", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--region", type=str, required=True, help="S3 bucket region")
    parser.add_argument("--delete", action="store_true", help="Delete objects after archiving")
    parser.add_argument("--dry-run", action="store_true", help="Simulate only, don't actually archive")
    parser.add_argument("--deep-archive", action="store_true", help="Use DEEP_ARCHIVE storage class")
    parser.add_argument("--file", type=str, help="Inventory file path (CSV)")
    parser.add_argument("--all", type=str, help="Add companies to archive")
    parser.add_argument("--profile", type=str, help="AWS profile to use")
    args = parser.parse_args()

    ############### Setup AWS ###############
    log_info(f"Configuring AWS with profile {args.profile} and region {args.region}")
    session = boto3.Session(region_name=args.region, profile_name=args.profile)
    s3client = session.client("s3")

    ########### Setup File ###############
    files = deque()

    if args.file:
        files.append(process_file(args.file))
    elif args.all:
        company_dirs = os.listdir(args.all)
        for company_dir in company_dirs:
            files.append(process_file(f"{args.all}/{company_dir}")) 

        print(f"files: {files}")
        return

    else:
        log_error("Inventory file path is required")
        return

    print(f"files: {files}")

    return 

    archived = False
    deleted = False

    ########### Process objects ###############
    archived = archive_object(
        region=args.region,
        profile=args.profile,
        bucket=args.bucket,
        dst_path=file_path,
        src_csv_path=file_path,
        deep_archive=args.deep_archive,
        dry_run=args.dry_run,
    )

    if archived:
        deleted = delete_object(
            s3client=s3client,
            bucket=args.bucket,
            src_path=file_path,
            delete=args.delete,
            dry_run=args.dry_run,
        )

    if deleted:
        os.remove(file_path)
        log_success(f"Archived and deleted {file_path}")
    else:
        log_success(f"Archived {file_path}")


if __name__ == "__main__":
    main()
