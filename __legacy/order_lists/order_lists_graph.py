import os
import sys
import logging
import requests
import pandas as pd

# Add your project root if needed
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from auth_helper import get_token

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# -- Set your DRIVE_ID for 'Documents' library here --
DRIVE_ID = "b!Aw3US9PgWEqI3AsHsnFh4VDpZqG7xMZFvusMriy0vjlVvmF4ME5wR6vdOy90ye56"

# -- Folder IDs for your order list roots --
TARGET_FOLDERS = [
    ("USA CUSTOMERS",   "01TP4AHIF77VK3XMDH4BHYQUXVCLJD62AU"),
    ("AUS Customers",   "01TP4AHIHDFBKGCMAO3BA37OFF7YUYSZ2N"),
    ("EU CUSTOMERS",    "01TP4AHIDKQKX2RK27AVAI2LFBCG5AFIBZ"),
    ("Lorna Jane",      "01TP4AHIBRQJL52ZEX5ZB2R6KVY4W6VMHJ"),
]

def find_order_lists(drive_id, folder_id, headers, max_depth=5, depth=0, found=None):
    if found is None:
        found = []
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children"
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        items = resp.json().get("value", [])
        for item in items:
            name = item.get("name", "")
            is_folder = "folder" in item
            if is_folder and depth + 1 < max_depth:
                find_order_lists(drive_id, item["id"], headers, max_depth, depth+1, found)
            elif (
                name.lower().endswith(".xlsx")
                and "order list (m3)" in name.lower()
            ):
                found.append({
                    "Name": name,
                    "FileURL": item.get("webUrl", ""),
                    "Path": item.get("parentReference", {}).get("path", ""),
                })
        url = resp.json().get("@odata.nextLink")
    return found

def main():
    token = get_token()
    if not token:
        logging.error("Failed to acquire Graph access token")
        return

    headers = {"Authorization": f"Bearer {token}"}

    all_found = []
    for region, folder_id in TARGET_FOLDERS:
        logging.info(f"Searching in: {region} ...")
        files = find_order_lists(DRIVE_ID, folder_id, headers, max_depth=5)
        for f in files:
            f["Region"] = region
        all_found.extend(files)

    if not all_found:
        logging.warning("No ORDER LIST (M3) Excel files found in any region.")
        return

    df = pd.DataFrame(all_found)[["Region", "Name", "Path", "FileURL"]]
    print("\n=== Matched ORDER LIST (M3) files across all regions ===")
    print(df.to_string(index=False))

if __name__ == "__main__":
    main()
