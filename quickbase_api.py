import requests
import os
from dotenv import load_dotenv


def get_qb_data():

    load_dotenv()

    realm_id = os.environ["QB_REALMID"]
    user_token = os.environ["QB_USER_TOKEN"]
    table_id = os.environ["QB_TABLEID"]
    page_size = int(os.environ.get("PAGE_SIZE", "10"))

    url = "https://api.quickbase.com/v1/records/query"

    headers = {
        "QB-Realm-Hostname": realm_id,
        "Authorization": f"QB-USER-TOKEN {user_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "from": table_id,    
        "select": [],            
        "options": {"top": page_size, "skip": 0}
    }


    response = requests.post(url= url, headers= headers, json = payload, timeout=60)
    response.raise_for_status()
    data = response.json()
    records = data.get("data", [])
    all_records = []
    skip = 0
    page = 0
    if len(records) > 0:
        page = 1
    
    print("Pages Extarcted:", page)

    all_records.extend(records)

    while len(records) == payload["options"]["top"]:
        
        skip+=page_size
        payload["options"]["skip"] = skip

        response = requests.post(url= url, headers= headers, json = payload, timeout=60)
        response.raise_for_status()
        data = response.json()
        records = data.get("data", [])
        all_records.extend(records)
        page+=1
        print("Pages Extracted:", page)

    print("Total records:", len(all_records))

    return all_records
