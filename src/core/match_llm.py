import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from llm_client import propose_links

def match(orders, ships):
    links = propose_links(orders, ships)
    if not links:
        return pd.DataFrame(), ships
    rows = []
    for link in links:
        o = orders.iloc[link["order_index"]].to_dict()
        s = ships.iloc[link["shipment_index"]].to_dict()
        rows.append({**o, **s, "method":"llm", "confidence":link["confidence"]})
    matched = pd.DataFrame(rows)
    ships_left = ships.drop(index=[l["shipment_index"] for l in links])
    return matched, ships_left
