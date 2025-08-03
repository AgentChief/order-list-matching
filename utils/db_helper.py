"""
Lightweight SQL helper for Kestra/data pipelines.
- Loads DB credentials from config.yaml in the working directory
- Uses ODBC Driver 17 for SQL Server
- Provides canonical customer name transformation
"""

import os
import pyodbc
import pandas as pd
import yaml
from pathlib import Path
from typing import Union, Optional

import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# --------------------------
# CONSTANTS
# --------------------------
DEFAULT_DRIVER = "{ODBC Driver 17 for SQL Server}"

# --------------------------
# CONFIG LOADING
# --------------------------

# -- Set config path to always resolve relative to this file's location
CONFIG_PATH = os.getenv("DB_CONFIG_PATH", str(Path(__file__).parent.parent / "config" / "config.yaml"))

def load_config(path: str = CONFIG_PATH):
    """Load configuration from YAML file"""
    with open(path, "r") as f:
        config = yaml.safe_load(f)
        return config

def get_database_config(path: str = CONFIG_PATH):
    """Get database configuration (for backward compatibility)"""
    config = load_config(path)
    return config.get('databases', {})

def get_api_config(api_name: str, path: str = CONFIG_PATH):
    """Get API configuration for specified API"""
    config = load_config(path)
    return config.get('apis', {}).get(api_name, {})

# Load database config for backward compatibility
DB_CONFIG = get_database_config()

# --------------------------
# CONNECTION FACTORY
# --------------------------
def get_connection(db_key: str) -> pyodbc.Connection:
    """
    Create a pyodbc connection using config.yaml block for the given db_key.
    Supports both conn_str format and individual key format.
    Tries ODBC Driver 17 first, falls back to SQL Server if not available.
    """
    cfg = DB_CONFIG[db_key.lower()]
    
    # Check if using conn_str format (legacy compatibility)
    if 'conn_str' in cfg:
        conn_str = cfg['conn_str'].strip()
        return pyodbc.connect(conn_str)
    
    # Use individual key format
    driver = cfg.get("driver", DEFAULT_DRIVER)
    
    # If using default driver, try ODBC Driver 17 first, then fall back to SQL Server
    if driver == DEFAULT_DRIVER:
        drivers_to_try = ["{ODBC Driver 17 for SQL Server}", "{SQL Server}"]
    else:
        drivers_to_try = [driver]
    
    conn_parts_base = [
        f"SERVER={cfg['host']},{cfg['port']}",
        f"DATABASE={cfg['database']}"
    ]

    if cfg.get('trusted_connection', '').lower() in ('yes', 'true', '1'):
        conn_parts_base.append("Trusted_Connection=yes")
    else:
        conn_parts_base.append(f"UID={cfg['username']}")
        conn_parts_base.append(f"PWD={cfg['password']}")

    encrypt = cfg.get('encrypt', 'yes').lower()
    conn_parts_base.append(f"Encrypt={'yes' if encrypt in ('yes', 'true', '1') else 'no'}")

    trust_cert = cfg.get('trustServerCertificate', 'no').lower()
    conn_parts_base.append(f"TrustServerCertificate={'yes' if trust_cert in ('yes', 'true', '1') else 'no'}")

    conn_parts_base.append("Connection Timeout=30")
    
    # Try each driver in order
    last_error = None
    for driver_attempt in drivers_to_try:
        try:
            conn_parts = [f"DRIVER={driver_attempt}"] + conn_parts_base
            conn_str = ";".join(conn_parts) + ";"
            return pyodbc.connect(conn_str)
        except pyodbc.InterfaceError as e:
            last_error = e
            # If this is a driver not found error, try the next driver
            if "Data source name not found" in str(e) or "IM002" in str(e):
                continue
            else:
                # Different error, re-raise immediately
                raise
        except Exception as e:
            # Non-driver related error, re-raise immediately
            raise
    
    # If we get here, all drivers failed
    raise last_error if last_error else Exception("No suitable ODBC driver found")

# --------------------------
# QUERY HELPERS
# --------------------------
def run_query(
    sql_or_path: Union[str, 'Path'],
    db_key: str,
    params: Optional[tuple] = None,
    index_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Run a SELECT SQL (inline or .sql file) and return DataFrame.
    """
    if isinstance(sql_or_path, str) and sql_or_path.strip().lower().endswith(".sql"):
        with open(sql_or_path, "r") as f:
            query = f.read()
    else:
        query = sql_or_path

    with get_connection(db_key) as conn:
        return pd.read_sql(query, conn, params=params, index_col=index_col)

def execute(
    sql_or_path: Union[str, 'Path'],
    db_key: str,
    params: Optional[tuple] = None,
    commit: bool = True
) -> int:
    """
    Execute (INSERT/UPDATE/DELETE). Returns affected row count.
    """
    if isinstance(sql_or_path, str) and sql_or_path.strip().lower().endswith(".sql"):
        with open(sql_or_path, "r") as f:
            query = f.read()
    else:
        query = sql_or_path

    with get_connection(db_key) as conn, conn.cursor() as cur:
        cur.execute(query, params or ())
        rowcount = cur.rowcount
        if commit:
            conn.commit()
        return rowcount

# --------------------------
# MIGRATION HELPERS
# --------------------------
def run_migration(
    migration_path: Union[str, Path],
    db_key: str = 'UNIFIED_ORDERS',
    verbose: bool = True
) -> bool:
    """
    Execute a SQL migration file against the specified database.
    
    Args:
        migration_path: Path to the .sql migration file
        db_key: Database key from config.yaml (default: UNIFIED_ORDERS)
        verbose: Whether to print progress messages
        
    Returns:
        bool: True if migration succeeded, False otherwise
    """
    try:
        migration_path = Path(migration_path)
        
        if not migration_path.exists():
            raise FileNotFoundError(f"Migration file not found: {migration_path}")
        
        if verbose:
            print(f"🔄 Running migration: {migration_path.name}")
            print(f"📊 Database: {db_key}")
        
        # Read the migration script
        with open(migration_path, 'r', encoding='utf-8') as f:
            script = f.read()
        
        if not script.strip():
            raise ValueError(f"Migration file is empty: {migration_path}")
        
        # Execute the migration
        with get_connection(db_key) as conn:
            cursor = conn.cursor()
            cursor.execute(script)
            conn.commit()
            cursor.close()
        
        if verbose:
            print(f"✅ Migration completed successfully: {migration_path.name}")
        
        return True
        
    except Exception as e:
        if verbose:
            print(f"❌ Migration failed: {migration_path.name if 'migration_path' in locals() else migration_path}")
            print(f"🔥 Error: {str(e)}")
        return False

def run_migrations_directory(
    migrations_dir: Union[str, Path],
    db_key: str = 'UNIFIED_ORDERS',
    pattern: str = "*.sql",
    verbose: bool = True
) -> dict:
    """
    Execute all SQL migration files in a directory in alphabetical order.
    
    Args:
        migrations_dir: Path to directory containing migration files
        db_key: Database key from config.yaml (default: UNIFIED_ORDERS)
        pattern: File pattern to match (default: *.sql)
        verbose: Whether to print progress messages
        
    Returns:
        dict: Results summary with 'successful', 'failed', and 'details' keys
    """
    migrations_dir = Path(migrations_dir)
    
    if not migrations_dir.exists():
        raise FileNotFoundError(f"Migrations directory not found: {migrations_dir}")
    
    # Find all migration files and sort them
    migration_files = sorted(migrations_dir.glob(pattern))
    
    if not migration_files:
        if verbose:
            print(f"⚠️  No migration files found in: {migrations_dir}")
        return {'successful': 0, 'failed': 0, 'details': []}
    
    if verbose:
        print(f"🚀 Running {len(migration_files)} migrations from: {migrations_dir}")
        print("=" * 60)
    
    results = {'successful': 0, 'failed': 0, 'details': []}
    
    for migration_file in migration_files:
        success = run_migration(migration_file, db_key, verbose)
        
        result_detail = {
            'file': migration_file.name,
            'success': success,
            'path': str(migration_file)
        }
        
        if success:
            results['successful'] += 1
        else:
            results['failed'] += 1
        
        results['details'].append(result_detail)
        
        if verbose:
            print("-" * 60)
    
    if verbose:
        print(f"\n📊 Migration Summary:")
        print(f"   ✅ Successful: {results['successful']}")
        print(f"   ❌ Failed: {results['failed']}")
        print(f"   📁 Total: {len(migration_files)}")
    
    return results
