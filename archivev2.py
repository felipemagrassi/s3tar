import logging
import boto3
import botocore
import sqlite3
import argparse
import os
import subprocess
import csv
from collections import deque
import json
from datetime import datetime


class Config:
    def __init__(self):
        self.bucket = None
        self.region = None
        self.profile = None
        self.delete = False
        self.dry_run = False
        self.deep_archive = False
        self.limit = 0
        self.archive = False


    def load(self, args: argparse.Namespace):
        self.bucket = args.bucket
        self.region = args.region
        self.profile = args.profile
        self.delete = args.delete
        self.dry_run = args.dry_run
        self.deep_archive = args.deep_archive
        self.limit = args.limit if args.limit is not None else 0
        self.archive = args.archive
    def __str__(self):
        return f"Config(bucket={self.bucket}, region={self.region}, profile={self.profile}, delete={self.delete}, dry_run={self.dry_run}, deep_archive={self.deep_archive})"


logging.basicConfig(
    level=logging.DEBUG, format="%(message)s", handlers=[logging.StreamHandler()]
)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def log_query(message: str) -> None:
    logging.info(f"🔍 QUERY: {message}")

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
    s3client: boto3.client,
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
        log_dry_run(
            f"Simulating archiving objects from {src_csv_path} to {full_dst_path}"
        )
        return True

    try:
        s3client.head_object(Bucket=bucket, Key=full_dst_path)
        log_info(f"Arquivo {full_dst_path} já existe, pulando")
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            log_info(f"Arquivo {full_dst_path} não existe, criando")
        else:
            log_error(f"Erro ao verificar se o arquivo existe: {e}")
            return False

    cmd = [
        "s3tar",
        "-vvv",
        "-c",
        "-f",
        full_dst_path,
        "--region",
        region,
        "--concat-in-memory",
        "-m",
        src_csv_path,
    ]

    if deep_archive:
        cmd.append("--storage-class")
        cmd.append("DEEP_ARCHIVE")

    if profile:
        cmd.append("--profile")
        cmd.append(profile)

    try:
        log_info(f"Executando comando: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        log_success(
            f"Arquivos de {src_csv_path} para {full_dst_path} arquivados com sucesso"
        )
        return True
    except subprocess.CalledProcessError as e:
        log_error(f"Erro ao arquivar arquivos de {src_csv_path}: {e}")
        log_error(e.stderr)
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

    log_info(f"Excluindo {src_path}")

    try:
        with open(src_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            objects_batch = []
            batch_size = 1000

            for row in csv_reader:
                if not row:
                    continue

                object_key = clean_string(row[1])
                if not object_key:
                    continue

                objects_batch.append({"Key": object_key})

                if len(objects_batch) >= batch_size:
                    log_info(f"Deletando lote de {len(objects_batch)} objetos")
                    s3client.delete_objects(
                        Bucket=bucket, Delete={"Objects": objects_batch}
                    )
                    objects_batch = []

            if objects_batch:
                log_info(f"Deletando lote final de {len(objects_batch)} objetos")
                s3client.delete_objects(
                    Bucket=bucket, Delete={"Objects": objects_batch}
                )

        log_info(f"Arquivo CSV {src_path} removido após processamento")
        return True

    except FileNotFoundError:
        log_error(f"Arquivo CSV não encontrado: {src_path}")
        return False
    except Exception as e:
        log_error(f"Erro ao processar arquivo CSV {src_path}: {e}")
        return False


def clean_string(string: str) -> str:
    return string.strip().replace('"', "")


def build_dst_path(file_path: str, config: Config) -> str:
    base_name = os.path.basename(file_path)

    dir_part = os.path.dirname(file_path)
    if dir_part.startswith("output/"):
        dir_part = dir_part[7:]

    transformed_dir = dir_part.replace(".", "/")

    transformed_path = os.path.join(transformed_dir, base_name)

    if transformed_path.startswith("/"):
        transformed_path = transformed_path[1:]

    return transformed_path


def calculate_day_difference(year: int, month: int, day: int) -> int:
    return (datetime.now() - datetime(year, month, day)).days

def build_csv(db: sqlite3.Connection, dst_path: str, s3client: boto3.client) -> str:
    cursor = db.cursor()
    cursor.execute(
        "SELECT bucket, path, size, date FROM s3_paths WHERE destination_path = ?",
        (dst_path,),
    )
    paths = cursor.fetchall()

    temp_csv_path = os.path.join("tmp", f"{dst_path[0].replace('.tar', '.csv')}")
    os.makedirs(os.path.dirname(temp_csv_path), exist_ok=True)
    with open(temp_csv_path, "w") as temp_csv_file:
        writer = csv.writer(temp_csv_file)
        for bucket, path, size, date in paths:
            if size > 0:
                writer.writerow([bucket, path, size, date])
    
    with open(temp_csv_path, "r") as temp_csv_file:
        char = temp_csv_file.read(1)
        if not char:
            log_info(f"Arquivo {temp_csv_path} vazio, pulando")
            query = "UPDATE s3_paths SET ignored = 1 WHERE destination_path = ?"
            cursor.execute(query, (dst_path[0],))
            db.commit()
            return None
        else:
            return temp_csv_path

def validate_destination_path(db: sqlite3.Connection, destination_path: str) -> bool:
    cursor = db.cursor()
    date = destination_path.split("/")[-1].replace(".tar", "")
    year, month, day = int(date.split("-")[0]), int(date.split("-")[1]), int(date.split("-")[2])
    if "raw" not in destination_path:
        log_info(f"Destination path {destination_path} is not a raw path, ignoring")
        cursor.execute("UPDATE s3_paths SET ignored = 1 WHERE destination_path = ?", (destination_path,))
        return False
    if day == 1:
        log_info(f"Destination path {destination_path} is the first day of the month, ignoring")
        cursor.execute("UPDATE s3_paths SET ignored = 1 WHERE destination_path = ?", (destination_path,))
        return False
    if not calculate_day_difference(year, month, day) > 90:
        log_info(f"Destination path {destination_path} is not older than 90 days, ignoring")
        cursor.execute("UPDATE s3_paths SET ignored = 1 WHERE destination_path = ?", (destination_path,))
        return False
    return True

def process_delete(db: sqlite3.Connection, config: Config, s3client: boto3.client) -> str:
    cursor = db.cursor()
    query = "SELECT destination_path FROM s3_paths WHERE (archived = 1 AND deleted = 0 AND ignored = 0) group by destination_path"
    log_query(query)
    cursor.execute(query)
    destination_paths = cursor.fetchall()
    for dst_path in destination_paths:
        log_info(f"Processing destination path: {dst_path}")
        if not validate_destination_path(db, dst_path):
            continue
        temp_csv_path = build_csv(db, dst_path, s3client)
        if temp_csv_path is None:
            continue
        deleted = delete_object(
            s3client=s3client,
            bucket=config.bucket,
            src_path=temp_csv_path,
            delete=config.delete,
            dry_run=config.dry_run,
        )

        if deleted:
            if not config.dry_run:
                cursor.execute(
                    "UPDATE s3_paths SET deleted = 1 WHERE destination_path = ?",
                    (dst_path[0],),
                )
                db.commit()

        os.remove(temp_csv_path)

def process_archive(db: sqlite3.Connection, config: Config, s3client: boto3.client) -> str:
    cursor = db.cursor()
    if config.limit > 0:
        query = f"SELECT destination_path FROM s3_paths WHERE (archived = 0 AND deleted = 0 AND ignored = 0) group by destination_path limit {config.limit}"
        log_query(query)
        cursor.execute(
            query,
        )
    else:
        query = "SELECT destination_path FROM s3_paths WHERE (archived = 0 AND deleted = 0 AND ignored = 0) group by destination_path"
        log_query(query)
        cursor.execute(
            query,
        )
    destination_paths = cursor.fetchall()
    for dst_path in destination_paths:
        log_info(f"Processing destination path: {dst_path[0]}")
        if not validate_destination_path(db, dst_path[0]):
            continue
        temp_csv_path = build_csv(db, dst_path[0], s3client)
        if temp_csv_path is None:
            continue

        archived = archive_object(
            region=config.region,
            profile=config.profile,
            bucket=config.bucket,
            dst_path=dst_path[0],
            src_csv_path=temp_csv_path,
            deep_archive=config.deep_archive,
            dry_run=config.dry_run,
            s3client=s3client,
        )

        if archived:
            if not config.dry_run:
                cursor.execute(
                    "UPDATE s3_paths SET archived = 1 WHERE destination_path = ?",
                    (dst_path[0],),
                )
                db.commit()
        
        os.remove(temp_csv_path)
    
def process(db: sqlite3.Connection, config: Config, s3client: boto3.client) -> str:
    if config.archive:
        process_archive(db, config, s3client)
    elif config.delete:
        process_delete(db, config, s3client)
    else:
        log_error("Nenhuma ação selecionada")


def main():
    ###### Arguments ######
    parser = argparse.ArgumentParser(
        description="Archive S3 objects from inventory files into TAR archives"
    )
    parser.add_argument("--bucket", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--region", type=str, required=True, help="S3 bucket region")
    parser.add_argument(
        "--delete", action="store_true", help="Delete objects after archiving"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Simulate only, don't actually archive"
    )
    parser.add_argument(
        "--deep-archive", action="store_true", help="Use DEEP_ARCHIVE storage class"
    )
    parser.add_argument("--profile", type=str, help="AWS profile to use")
    parser.add_argument("--limit", type=int, help="Limit the number of destination paths to process")
    parser.add_argument("--archive", action="store_true", help="Archive objects")
    args = parser.parse_args()

    ############### Setup AWS ###############
    log_info(f"Configuring AWS with profile {args.profile} and region {args.region}")
    session = boto3.Session(region_name=args.region, profile_name=args.profile)
    s3client = session.client("s3")

    ########### Cache Setup ###############
    db = sqlite3.connect("s3_paths.db")

    ########### Setup AWS ###############
    config = Config()
    config.load(args)

    ########### Setup Files ###############

    process(db, config, s3client)


if __name__ == "__main__":
    main()
