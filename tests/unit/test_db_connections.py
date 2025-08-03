import sys
from pathlib import Path
import yaml
import pyodbc

def ensure_encryption(conn_str):
    # Ensure Encrypt and TrustServerCertificate are present
    if "Encrypt=" not in conn_str:
        conn_str += ";Encrypt=yes"
    if "TrustServerCertificate=" not in conn_str:
        conn_str += ";TrustServerCertificate=yes"
    return conn_str

# Load config
config_path = Path(__file__).parent / "config" / "config.yaml"
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

def test_connection(db_key, table_name):
    print(f"\nTesting connection for '{db_key}'...")
    conn_str = config["databases"][db_key]["conn_str"]
    conn_str = ensure_encryption(conn_str)
    print(f"Using connection string:\n{conn_str}\n")
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        print(f"Connected to {db_key} database.")
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
            row = cursor.fetchone()
            if row:
                print(f"Sample row from {table_name}: {row}")
            else:
                print(f"Table {table_name} exists but is empty.")
        except Exception as qe:
            print(f"Query error for table '{table_name}': {qe}")
        finally:
            cursor.close()
            conn.close()
    except Exception as ce:
        print(f"Connection error for '{db_key}': {ce}")

if __name__ == "__main__":
    test_connection("orders", "ORDER_LIST")
    test_connection("orders", "FM_orders_shipped")
