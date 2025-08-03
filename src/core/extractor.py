# ── src/core/extractor.py ─────────────────────────────────────────────
"""
Uses utils.db_helper.run_query() so we rely solely on pyodbc + pandas.

config.yaml must have   databases: { orders: {...}, shipments: {...} }
with host / port / database / username / password keys, exactly as
expected by your db_helper.
"""
import sys
from pathlib import Path
from ruamel.yaml import YAML
from datetime import datetime, timedelta

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent.parent / "utils"))
from db_helper import run_query
import pandas as pd

def _sql_in(values) -> str:
    """Return a single‑quoted, comma‑separated list, SQL‑escaped."""
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)

def _format_date_filter(date_from=None, date_to=None, date_column="Shipped_Date"):
    """Generate SQL date filter clause"""
    if not date_from and not date_to:
        return ""
    
    conditions = []
    if date_from:
        conditions.append(f"[{date_column}] >= '{date_from}'")
    if date_to:
        conditions.append(f"[{date_column}] <= '{date_to}'")
    
    return " AND " + " AND ".join(conditions)

# ---------------------------------------------------------------------
def orders(customer_aliases, po=None) -> pd.DataFrame:
    """Extract orders. If po is None, gets all orders for customer."""
    where_clauses = [f"[CUSTOMER NAME] IN ({_sql_in(customer_aliases)})"]
    params = []
    
    if po:
        where_clauses.append("[PO NUMBER] = ?")
        params.append(po)
    
    sql = f"""
    SELECT *
    FROM ORDERS_UNIFIED
    WHERE {' AND '.join(where_clauses)}
    """
    return run_query(sql, db_key="orders", params=tuple(params))

def shipments(customer_aliases, po=None, date_from=None, date_to=None) -> pd.DataFrame:
    """Extract shipments with optional date filtering."""
    where_clauses = [f"Customer IN ({_sql_in(customer_aliases)})"]
    params = []
    
    if po:
        where_clauses.append("Customer_PO = ?")
        params.append(po)
    
    # Add date filtering
    date_filter = _format_date_filter(date_from, date_to, "Shipped_Date")
    
    sql = f"""
    SELECT *
    FROM FM_orders_shipped
    WHERE {' AND '.join(where_clauses)}{date_filter}
    ORDER BY Shipped_Date DESC
    """
    return run_query(sql, db_key="shipments", params=tuple(params))

def shipments_by_date_range(customer_aliases, date_from, date_to) -> pd.DataFrame:
    """Get all shipments for a customer within a date range, regardless of PO."""
    date_filter = _format_date_filter(date_from, date_to, "Shipped_Date")
    
    sql = f"""
    SELECT *
    FROM FM_orders_shipped
    WHERE Customer IN ({_sql_in(customer_aliases)}){date_filter}
    ORDER BY Shipped_Date DESC, Customer_PO
    """
    return run_query(sql, db_key="shipments", params=())
# ─────────────────────────────────────────────────────────────────────
