import logging
import requests
from auth_helper import get_token

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MAX_DEPTH = 3  # Set how many layers deep to go

def print_drive_tree(drive_id, folder_path, headers, depth=0):
    from urllib.parse import quote

    # Compose path for Graph (root if folder_path == "")
    if folder_path:
        quoted = quote(folder_path.strip("/"), safe="/")
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{quoted}:/children"
    else:
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"

    resp = requests.get(url, headers=headers)
    if resp.status_code == 404:
        print(" " * (2 * depth) + f"[404] {folder_path} (does not exist)")
        return
    resp.raise_for_status()
    data = resp.json()

    for entry in data.get("value", []):
        is_folder = "folder" in entry
        indent = "  " * depth
        print(f"{indent}{'[DIR]' if is_folder else '[FILE]'} {entry['name']} | ID: {entry['id']}")
        # If it's a folder and we haven't reached max depth, go deeper
        if is_folder and depth + 1 < MAX_DEPTH:
            sub_path = f"{folder_path.rstrip('/')}/{entry['name']}" if folder_path else entry['name']
            print_drive_tree(drive_id, sub_path, headers, depth + 1)

def main():
    token = get_token()
    if not token:
        logging.error("Failed to acquire Graph access token")
        return

    headers = {"Authorization": f"Bearer {token}"}

    # Resolve site-id for /sites/DataAdmin
    site_path = "activeapparelgroup.sharepoint.com:/sites/DataAdmin"
    url_site = f"https://graph.microsoft.com/v1.0/sites/{site_path}"
    resp = requests.get(url_site, headers=headers)
    resp.raise_for_status()
    site_id = resp.json()["id"]
    logging.info(f"Using site-id: {site_id}")

    # List all document libraries (drives)
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    drives = resp.json().get("value", [])

    # Find "Documents" drive
    doc_drive = next((d for d in drives if d['name'].lower() == 'documents'), None)
    if not doc_drive:
        logging.error("No 'Documents' library found.")
        return

    drive_id = doc_drive['id']
    print(f"\n=== FOLDER TREE in 'Documents' (Drive ID: {drive_id}) ===")
    print_drive_tree(drive_id, "", headers, depth=0)

if __name__ == "__main__":
    main()
