from quickbase_api import get_qb_data
from upload_to_gcs import upload_records_jsonl_to_gcs

if __name__ == "__main__":
    records = get_qb_data()
    print(upload_records_jsonl_to_gcs(records))


