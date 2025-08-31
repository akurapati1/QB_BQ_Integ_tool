import os
import io
import json
import gzip
from datetime import datetime
from typing import List, Dict, Optional
from google.cloud import storage
from dotenv import load_dotenv
from google.oauth2 import service_account

def upload_records_jsonl_to_gcs(records: List[Dict]) -> str:
    bucket_name = os.environ["BUCKET"]
    compress = os.getenv("COMPRESS_JSONL", "true").lower() == "true"
    prefix = os.getenv("GCS_PREFIX", "quickbase_exports/")
    project = os.getenv("PROJECT_NAME")
    credentials_path = os.environ["GCP_CREDENTIALS_PATH"]
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    ext = "jsonl.gz" if compress else "jsonl"
    blob_name = f"{prefix.rstrip('/')}/qb_records_{ts}.{ext}"

    client = storage.Client(credentials=credentials, project=project)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.chunk_size = 5 * 1024 * 1024

    if compress:
        blob.content_type = "application/gzip"
        blob.content_encoding = "gzip"
    else:
        blob.content_type = "application/x-ndjson"

    # Write bytes to avoid TextIOWrapper/flush issues
    with blob.open("wb") as raw_fh:
        if compress:
            with gzip.GzipFile(fileobj=raw_fh, mode="wb") as gz_fh:
                for rec in records:
                    line = json.dumps(rec, ensure_ascii=False, default=str) + "\n"
                    gz_fh.write(line.encode("utf-8"))
        else:
            for rec in records:
                line = json.dumps(rec, ensure_ascii=False, default=str) + "\n"
                raw_fh.write(line.encode("utf-8"))

    return f"gs://{bucket_name}/{blob_name}"
