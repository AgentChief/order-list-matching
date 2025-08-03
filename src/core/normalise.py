import re, pandas as pd
from ruamel.yaml import YAML
from pathlib import Path

yaml = YAML(typ="safe")
# Get the path to the project root (order-match-lm/) from this file's location
project_root = Path(__file__).parent.parent.parent
RULES = yaml.load((project_root / "config" / "customer_rules.yaml").read_text())

def _upper_trim(x): return re.sub(r"\s+", " ", str(x).upper()).strip()

def orders(df: pd.DataFrame, cust: str):
    df["CUSTOMER NAME"] = df["CUSTOMER NAME"].map(_upper_trim)
    df["CUSTOMER STYLE"] = df["CUSTOMER STYLE"].map(_upper_trim)
    df["CUSTOMER COLOUR DESCRIPTION"] = df["CUSTOMER COLOUR DESCRIPTION"].map(_upper_trim)
    return _drop_sizes(df, cust)

def shipments(df: pd.DataFrame, cust: str):
    df["Customer"] = df["Customer"].map(_upper_trim)
    df["Style"] = df["Style"].map(_upper_trim)
    df["Color"] = df["Color"].map(_upper_trim)
    return _drop_sizes(df, cust)

def _drop_sizes(df, cust):
    excl = set(RULES.get(cust, {}).get("exclude_sizes", RULES["defaults"]["exclude_sizes"]))
    if "Size" in df.columns:
        df = df[~df["Size"].astype(str).str.upper().isin(excl)]
    # Don't reset index to preserve original row tracking
    return df
