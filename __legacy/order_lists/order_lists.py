import os
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

# ── 1. AUTHENTICATE ───────────────────────────────────────────────────────────────
site_url      = "https://activeapparelgroup.sharepoint.com/sites/DataAdmin"
client_id     = "69b0a83d-a41c-4b72-93f3-aed8a6563b41"
client_secret = "anI8Q~fIxMrIAVsK15NUrlCDCAsbzE3Lusd.5b0J"
client_id = "69b0a83d-a41c-4b72-93f3-aed8a6563b41"
client_secret = "anI8Q~fIxMrIAVsK15NUrlCDCAsbzE3Lusd.5b0J"


ctx = ClientContext(site_url).with_credentials(
    ClientCredential(client_id, client_secret)
)

# ── 2. LIST ALL FILES IN THE TARGET FOLDER ────────────────────────────────────────
folder = ctx.web.get_folder_by_server_relative_url("Shared Documents/DataAdmin")
files  = folder.files
ctx.load(files)
ctx.execute_query()

# Build a DataFrame of file metadata
records = []
for f in files:
    p = f.properties
    # derive folder path (serverRelativeUrl minus filename)
    server_url = p["ServerRelativeUrl"]
    folder_path = server_url.rsplit("/", 1)[0] + "/"
    records.append({
        "Name": p["Name"],
        "Folder Path": folder_path,
        "ServerRelativeUrl": server_url,
        "Hidden": p.get("Hidden", False),      # adjust if the property is named differently
        "Content": None,                       # placeholder for binary content
        "Extension": os.path.splitext(p["Name"])[1].lower()
    })

df = pd.DataFrame.from_records(records)

# ── 3. APPLY M-QUERY TRANSFORMS ────────────────────────────────────────────────────

# Filter only .xlsx files
df = df[df["Extension"] == ".xlsx"]

# Filter rows where Name contains "ORDER LIST (M3)."
df = df[df["Name"].str.contains("ORDER LIST (M3)\.", regex=True)]

# Sort by Name ascending
df = df.sort_values("Name", ascending=True)

# Filter out hidden files
df = df[~df["Hidden"]]

# Remove rows where Name starts with "_"
df = df[~df["Name"].str.startswith("_")]

# Drop unwanted columns (Date accessed/modified/created & Attributes in M)
# here we only have the ones we built; if you loaded more metadata, drop them here
df = df.drop(columns=["ServerRelativeUrl", "Hidden", "Content", "Extension"], errors="ignore")

# Add FileURL = Folder Path + Name
base_site = "https://activeapparelgroup.sharepoint.com"
df["FileURL"] = df["Folder Path"].apply(lambda p: base_site + p) + df["Name"]

# Replace any errors in FileURL with None
df["FileURL"] = df["FileURL"].where(df["FileURL"].notnull(), None)

# Duplicate "Folder Path" → "Folder Path - Copy"
df["Folder Path - Copy"] = df["Folder Path"]

# Remove leading " Documents/" from that copy (same as M ReplaceValue)
df["Folder Path - Copy"] = df["Folder Path - Copy"].str.replace(" Documents/", "", regex=False)

# Split the copy by "/" into up to 8 parts
split_cols = df["Folder Path - Copy"].str.split("/", n=7, expand=True)
for idx in range(split_cols.shape[1]):
    df[f"Folder Path - Copy.{idx+1}"] = split_cols[idx]

# Drop the extra split columns .2 through .7 (we only keep .1)
to_drop = [f"Folder Path - Copy.{i}" for i in range(2, 8)]
df = df.drop(columns=to_drop)

# Filter out rows where Folder Path - Copy.1 == "DA_LOGISTICS - GLOBAL DISTRIBUTION"
df = df[df["Folder Path - Copy.1"] != "DA_LOGISTICS - GLOBAL DISTRIBUTION"]

# Finally drop the Folder Path - Copy.1 column
df = df.drop(columns=["Folder Path - Copy.1"])

# ── 4. RESULT ────────────────────────────────────────────────────────────────────
# 'df' now matches your #"Removed columns 3" table in M.
print(df.reset_index(drop=True))
