import logging
import requests
from auth_helper import get_token_serv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TARGET_FOLDERS = [
    ("USA CUSTOMERS",   "01TP4AHIF77VK3XMDH4BHYQUXVCLJD62AU"),
    ("AUS Customers",   "01TP4AHIHDFBKGCMAO3BA37OFF7YUYSZ2N"),
    ("_PAST Customers", "01TP4AHIGOKGHMV5XZORFYVBCXNLKAODWK"),
    ("EU CUSTOMERS",    "01TP4AHIDKQKX2RK27AVAI2LFBCG5AFIBZ"),
    ("Lorna Jane",      "01TP4AHIBRQJL52ZEX5ZB2R6KVY4W6VMHJ"),
]

DRIVE_ID = "b!Aw3US9PgWEqI3AsHsnFh4VDpZqG7xMZFvusMriy0vjlVvmF4ME5wR6vdOy90ye56"
MAX_DEPTH = 2
ORDER_LIST_FOLDER_MATCH = "order list"
ORDER_LIST_FILE_MATCH = "order list (m3)"

results = []

def search_for_order_list_file(drive_id, folder_id, headers, region, parent_name, subfolder_name):
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    items = resp.json().get("value", [])
    for item in items:
        name = item.get("name", "")
        if name.lower().endswith(".xlsx") and ORDER_LIST_FILE_MATCH in name.lower():
            results.append({
                "Region": region,
                "Parent": parent_name,
                "Subfolder": subfolder_name,
                "FileName": name,
                "FileURL": item.get("webUrl", "")
            })
            break

def find_and_accumulate(drive_id, region_name, parent_id, headers, depth=0):
    if depth >= MAX_DEPTH:
        return
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{parent_id}/children"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    items = resp.json().get("value", [])
    for item in items:
        if "folder" in item:
            folder_name = item["name"]
            parent_name = item['parentReference'].get('name', '?')
            if ORDER_LIST_FOLDER_MATCH in folder_name.lower():
                search_for_order_list_file(drive_id, item['id'], headers, region_name, parent_name, folder_name)
            find_and_accumulate(drive_id, region_name, item['id'], headers, depth + 1)
            count = sum(1 for r in results)
            logging.info(f"[Found {count} matching 'order list (m3)' files in '{folder_name}' subfolders.]")

def main():
    token = get_token_serv()
    if not token:
        logging.error("Failed to acquire Graph access token")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    for region, folder_id in TARGET_FOLDERS:
        find_and_accumulate(DRIVE_ID, region, folder_id, headers, depth=0)

    if not results:
        print("No matching 'order list (m3)' files found.")
        return

    print("\n=== FINAL FILE LIST ===")
    for r in results:
        print(f"[{r['Region']}] Parent: {r['Parent']:<25} | Subfolder: {r['Subfolder']:<25} | File: {r['FileName']} | URL: {r['FileURL']}")

if __name__ == "__main__":
    main()
