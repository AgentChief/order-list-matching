# ORDER_LIST Extract‑only Pipeline – matches original complete.py speed
# ---------------------------------------------------------------------
# * 1‑to‑1 refactor of complete.py *minus* all transform / comparison steps
# * Landing tables now follow x<BASENAME>_ORDER_LIST_RAW convention
# * Row‑count validation after each BULK INSERT
# * All constants, credentials, helper imports kept verbatim so nothing
#   else in your repo changes.

import os, io, re, sys, time, csv, warnings
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import List, Dict

import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, generate_container_sas, ContainerSasPermissions

# --- suppress openpyxl chatter ------------------------------------------------
warnings.filterwarnings(
    "ignore", message="Data Validation extension is not supported and will be removed"
)

# --- repo utils path setup ----------------------------------------------------
def find_repo_root() -> Path:
    """Find repository root by looking for pipelines/utils folder"""
    current = Path(__file__).resolve()
    while current.parent != current:
        if (current.parent.parent / "pipelines" / "utils").exists():
            return current.parent.parent
        current = current.parent
    raise RuntimeError("Could not find repository root with utils/ folder")

repo_root = find_repo_root()
sys.path.insert(0, str(repo_root / "pipelines" / "utils"))


import db_helper                 # noqa: E402
import schema_helper             # noqa: E402
import logger_helper             # noqa: E402

# ---------------- hard‑coded CONFIG (unchanged) -------------------------------
TENANT_ID     = "95d7583a-1925-44b1-b78b-3483e00c5b46"
CLIENT_ID     = "a9b37534-61b0-41f0-ab40-2855cd6aa5bb"
CLIENT_SECRET = "N.E8Q~YDZ67vyPgWXA~wuCkderVGQSf6V0S48a.Y"
ACCOUNT_NAME  = "aagorderlist2blob"
ACCOUNT_KEY   = "9ECtN/2bDhVwhVOD9N2nwYd954XHWPt2QOBoyOsmtjsu/KOGxEgQDRYu9p8IoiLbjV9bq4jpb3gT+AStRiH2lQ=="

SOURCE_CONTAINER = "orderlist"
TARGET_CONTAINER = "orderlistcsv"

DB_KEY            = "orders"
OVERWRITE_DB      = True
MAX_WORKERS       = 4  # keep sequential if you suspect throttling

# ---------------- blob client --------------------------------------------------
# MOVED TO MAIN FUNCTION - Don't initialize at module level!

# ---------------- helpers ------------------------------------------------------

def get_blob_clients():
    """Initialize blob clients when needed - not at module level"""
    blob_svc = BlobServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
        credential=ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET),
    )
    
    src_client = blob_svc.get_container_client(SOURCE_CONTAINER)
    trg_client = blob_svc.get_container_client(TARGET_CONTAINER)
    return blob_svc, src_client, trg_client

def safe_table_name(xlsx_name: str) -> str:
    name = re.sub(r"\.xls[x]?$", "", xlsx_name, flags=re.I)
    name = name.replace("(M3)", "").replace("'", "")
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^A-Za-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return f"x{name}_RAW"


def best_sheet(xlsx_bytes: bytes) -> str:
    xls = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    return "MASTER" if "MASTER" in xls.sheet_names else xls.sheet_names[0]


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in df.columns if c and not str(c).lower().startswith("unnamed")]
    df = df[cols]
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in cols]
    df = df.dropna(how="all")
    df = df.loc[~df.apply(lambda r: r.astype(str).str.strip().eq("").all(), axis=1)]
    return df


# ---------------- external DATA‑SOURCE once per run ---------------------------

def ensure_external_ds() -> None:
    conn = db_helper.get_connection(DB_KEY)
    cur  = conn.cursor()

    start  = datetime.now(UTC) - timedelta(minutes=10)
    expiry = start + timedelta(hours=2)
    sas = generate_container_sas(
        account_name   = ACCOUNT_NAME,
        container_name = TARGET_CONTAINER,
        account_key    = ACCOUNT_KEY,
        permission     = ContainerSasPermissions(read=True, list=True),
        start          = start,
        expiry         = expiry,
    )

    cur.execute("""
        IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name='CsvBlobSrc')
            DROP EXTERNAL DATA SOURCE CsvBlobSrc;
        IF EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name='AzureBlobCred')
            DROP DATABASE SCOPED CREDENTIAL AzureBlobCred;
    """)

    cur.execute(f"""
        CREATE DATABASE SCOPED CREDENTIAL AzureBlobCred
        WITH IDENTITY='SHARED ACCESS SIGNATURE', SECRET='{sas}';
        CREATE EXTERNAL DATA SOURCE CsvBlobSrc
        WITH (TYPE=BLOB_STORAGE,
              LOCATION='https://{ACCOUNT_NAME}.blob.core.windows.net/{TARGET_CONTAINER}',
              CREDENTIAL=AzureBlobCred);
    """)
    conn.commit()
    cur.close(); conn.close()

# ---------------- core loader --------------------------------------------------

def bulk_load(df: pd.DataFrame, table: str, trg_client) -> int:
    conn = db_helper.get_connection(DB_KEY)
    cur  = conn.cursor()

    if OVERWRITE_DB:
        cur.execute(f"IF OBJECT_ID('dbo.{table}','U') IS NOT NULL DROP TABLE dbo.{table}")
        conn.commit()

    cols_sql, schema = schema_helper.generate_table_schema(df)
    cur.execute(f"CREATE TABLE dbo.{table} ({', '.join(cols_sql)})")
    conn.commit()

    df_sql = schema_helper.convert_df_for_sql(df, schema)

    csv_buf = io.StringIO()
    df_sql.to_csv(csv_buf, index=False, quoting=csv.QUOTE_MINIMAL, lineterminator='\r\n', encoding='utf-8')
    blob_name = f"csv_temp/{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    trg_client.get_blob_client(blob_name).upload_blob(csv_buf.getvalue().encode('utf-8-sig'), overwrite=True)

    cur.execute(f"""
        BULK INSERT dbo.{table}
        FROM '{blob_name}'
        WITH (DATA_SOURCE='CsvBlobSrc', FORMAT='CSV', FIRSTROW=2,
              FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK)
    """)
    conn.commit()
    cur.execute(f"SELECT COUNT(*) FROM dbo.{table}")
    rows = cur.fetchone()[0]

    # -- change print to logger.info -- #
    logger_helper.info(f"[{table}] BULK INSERT: {rows:,} rows loaded from {blob_name}")

    cur.close(); conn.close()
    return rows

# ---------------- per‑file pipeline -----------------------------------------

def process_blob(blob_name: str, src_client, trg_client) -> Dict:
    start = time.time()

    data = src_client.get_blob_client(blob_name).download_blob().readall()
    df = pd.read_excel(io.BytesIO(data), sheet_name=best_sheet(data), dtype=str, na_filter=False)
    df = clean_df(df)
    df['_SOURCE_FILE']  = blob_name
    df['_EXTRACTED_AT'] = datetime.now(UTC)

    table = safe_table_name(blob_name)
    loaded = bulk_load(df, table, trg_client)

    return {
        'file': blob_name,
        'table': table,
        'rows_src': len(df),
        'rows_db': loaded,
        'match': len(df) == loaded,
        'elapsed': time.time() - start
    }

# ---------------- driver ------------------------------------------------------

def main():
    """ORDER_LIST Extract Phase: Blob storage → SQL tables"""
    
    # Get blob clients (lazy initialization for better performance)
    blob_svc, src_client, trg_client = get_blob_clients()
    
    print("\n[*] ORDER_LIST - RAW Landing Pipeline (extract-only)\n" + "="*60)
    ensure_external_ds()

    blobs = [b.name for b in src_client.list_blobs() if b.name.lower().endswith(('.xlsx', '.xls'))]
    results = [process_blob(b, src_client, trg_client) for b in blobs]  # sequential – safest; tune later

    ok = all(r['match'] for r in results)
    total_rows = sum(r['rows_db'] for r in results)
    total_time = sum(r['elapsed'] for r in results)

    print("[+] FINISHED" if ok else "[!] Completed with mismatches")
    print(f"Files         : {len(results)}")
    print(f"Rows loaded   : {total_rows:,}")
    print(f"Elapsed time  : {total_time:.2f}s  (~{total_rows/total_time:,.0f} rows/s)\n")

    print("File | Src | DB | Match | Secs\n" + "-"*45)
    for r in results:
        print(f"{r['file']} | {r['rows_src']} | {r['rows_db']} | {r['match']} | {r['elapsed']:.1f}")

    return {
        'files_processed': len(results),
        'success_count': sum(1 for r in results if r['match']),
        'total_rows': total_rows,
        'results': results
    }

if __name__ == "__main__":
    main()
