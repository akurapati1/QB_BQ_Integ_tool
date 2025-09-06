import requests
import os
from dotenv import load_dotenv

QB_FIELDS_URL = "https://api.quickbase.com/v1/fields"
QB_QUERY_URL  = "https://api.quickbase.com/v1/records/query"


def get_qb_data():

    load_dotenv()

    realm_id  = os.environ["QB_REALMID"]     
    user_token = os.environ["QB_USER_TOKEN"]
    table_id   = os.environ["QB_TABLEID"]
    page_size  = int(os.environ.get("PAGE_SIZE", "100"))

    headers = {
        "QB-Realm-Hostname": realm_id,
        "Authorization": f"QB-USER-TOKEN {user_token}",
        "Content-Type": "application/json",
    }

    # --- 1) Get field metadata (FID -> label) ---
    r = requests.get(QB_FIELDS_URL, headers=headers, params={"tableId": table_id}, timeout=60)
    r.raise_for_status()
    js = r.json()
    fields = js if isinstance(js, list) else js.get("fields", [])
    fid_to_label = {str(f.get("id")): (f.get("label") or f.get("fieldName") or str(f.get("id")))
                    for f in fields}

    # --- 2) Query ALL fields (select: ["a"]) with pagination ---
    payload = {
        "from": table_id,
        "select": ["a"],                         # ALL fields, not just report defaults
        "options": {"top": page_size, "skip": 0}
    }

    all_records = []

    # first page
    response = requests.post(url=QB_QUERY_URL, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()
    records = data.get("data", [])
    page = 1 if records else 0
    if page:
        print("Pages Extracted:", page)
    all_records.extend(records)

    # next pages
    while len(records) == page_size:
        payload["options"]["skip"] += page_size
        response = requests.post(url=QB_QUERY_URL, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        data = response.json()
        records = data.get("data", [])
        all_records.extend(records)
        page += 1
        print("Pages Extracted:", page)

    print("Total records:", len(all_records))

    # --- 3) Flatten cells and rename keys from FID -> label ---
    def _flatten(rec: dict) -> dict:
        out = {}
        for k, v in rec.items():
            if isinstance(v, dict) and "value" in v:
                out[k] = v["value"]
            else:
                out[k] = v
        return out

    flattened = [_flatten(r) for r in all_records]

    renamed = []
    for rec in flattened:
        out = {}
        for fid, val in rec.items():
            key = fid_to_label.get(str(fid), str(fid))  # fallback to fid if unknown
            out[key] = val
        renamed.append(out)
    
    

    return renamed

