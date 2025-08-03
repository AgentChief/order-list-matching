### Active Apparel Group

#### Shipment ⇄ Order ⇄ Mfg‑Plan Reconciliation System

**Product & Developer Specification – v0.9 (Day‑one MVP)**
*(last updated 29 Jul 2025)*

---

## 1 · High‑level goals

| #   | Goal                                                                                                                 |
| --- | -------------------------------------------------------------------------------------------------------------------- |
| G‑1 | Guarantee every **shipment line** is traced back to its corresponding **customer order** and **manufacturing plan**. |
| G‑2 | Provide an auditable classification: **EXACT\_OK · EXACT\_QTY\_MISMATCH · HI\_CONF · LOW\_CONF · NO\_MATCH**.        |
| G‑3 | Maintain a **Human‑in‑the‑Loop (HITL)** queue so planners approve new aliases and tolerances.                        |
| G‑4 | Store approved aliases centrally so tomorrow they become **deterministic Layer 0** matches.                          |
| G‑5 | Capture **daily snapshots** (forward‑looking) to unlock trend models later.                                          |

---

### 1.1 Human in the Loop (HITL) alias management

Ingest raw files
      │
      ▼
1️⃣  Canonicalise via  map_attribute  (approved aliases only)
      │         └─> Miss? → keep raw as-is
      ▼
2️⃣  Layer 0 deterministic match
      │            └─> Unmatched rows → Layer 1
      ▼
3️⃣  Layer 1 fuzzy scorer
      │
      ├─ score ≥ 0.85  ➜  auto-match  + write *HI_CONF* alias
      │                      ↳ insert into alias_review_queue (status=PENDING, confidence=0.85)
      │
      ├─ 0.60-0.85   ➜  LOW_CONF  → queue with status=PENDING
      │
      └─ <0.60       ➜  NO_MATCH  → queue with status=PENDING (optionally)


## 2 · Why *recordlinkage* (not rapidfuzz) for MVP

1. **End‑to‑end pipeline** – candidate blocking, per‑column similarity, scoring, evaluation.
2. **Extensible** – can switch from rule‑based weights to the built‑in logistic classifier without code rewrite.
3. **Still uses rapidfuzz internally** when we pass custom similarity functions, so we don’t lose speed.
4. **Audit‑friendly** – keeps a feature matrix for every candidate pair.

Rapidfuzz alone solves only string distance; we’d have to hand‑roll the rest.

---

## 3 · Current data landscape (Day 1)

```
dbo.orders_current                  -- 102 k rows, today only
dbo.manufacturing_plans_current     -- 100 k rows, today only
dbo.shipments_current               --  88 k rows, today only
```

All three already include: `customer_id · style_raw · colour_raw · po_raw · alt_po_raw · delivery_method_raw · qty`.
No historical snapshots exist YET.

---

## 4 · New database artefacts (Day 1)

| Table                                                                                                                                                                      | Purpose                                                                   |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| **map\_attribute** <br>`(customer_id, attr_type, raw_value, canon_value, confidence, approved_by, approved_ts)`                                                            | Approved aliases – the *source of truth* for canonicalisation.            |
| **alias\_review\_queue** <br>`(queue_id, customer_id, attr_type, raw_value, canon_value, confidence, status, suggested_by, suggested_ts, approved_by, approved_ts, notes)` | HITL work‑queue (status = PENDING / APPROVED / REJECTED).                 |
| **recon\_results\_tmp**                                                                                                                                                    | Temp table written by Python; consumed by SQL deduction script.           |
| **order\_list\_daily\_snapshot**                                                                                                                                           | (Phase 1) nightly snapshot of `orders_current` for time‑series modelling. |

---

## 5 · Tech stack

| Layer             | Tool / Lib                                                                                                      | Notes                                              |
| ----------------- | --------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| ETL orchestration | **Kestra**                                                                                                      | One flow per reconciliation cycle.                 |
| Python env        | **3.11**                                                                                                        | Poetry‑managed.                                    |
| Key libs          | `pandas` · `SQLAlchemy` · `recordlinkage` · `rapidfuzz` (optional) · `streamlit` · `pyodbc` · `river` (phase 2) |                                                    |
| UI                | **Streamlit**                                                                                                   | Internal alias‑manager @ `/alias‑ui`.              |
| CI/CD             | GitHub Actions                                                                                                  | test → docker build → push to ACR → Kestra deploy. |

---

## 6 · Repository layout

```
aag‑recon/
├── README.md
├── poetry.lock / pyproject.toml
├── config/
│   ├── db.yaml
│   └── recon.yml              # tolerance %, confidence cut‑offs
├── sql/
│   ├── 00_schema/
│   │   ├── 01_alias_tables.sql
│   │   └── 02_snapshot_table.sql
│   ├── 10_deduction/
│   │   └── apply_deductions.sql
│   └── 20_snapshot/
│       └── nightly_snapshot.sql
├── pipelines/
│   ├── flows/
│   │   └── reconcile_orders.yaml   # Kestra flow definition
│   └── scripts/
│       ├── recon.py               # Layer 0 & 1 logic (this file → details §7)
│       └── promote_aliases.py     # moves APPROVED → map_attribute
├── ui/
│   └── alias_manager.py           # Streamlit app
├── tests/
│   ├── test_recon_small.py
│   └── fixtures/
└── docker/
    └── Dockerfile
```

---

## 7 · `pipelines/scripts/recon.py` (core logic)

```python
"""
recon.py  –  Runs Layer‑0 + Layer‑1 reconciliation, writes recon_results_tmp.
"""
import pandas as pd, recordlinkage as rl
from sqlalchemy import create_engine
from utils.db import load_cfg_engine, write_temp

engine = load_cfg_engine()           # uses config/db.yaml

# 1 · Load sources -----------------------------------------------------------
orders = pd.read_sql("SELECT * FROM dbo.orders_current", engine)
mfg    = pd.read_sql("SELECT * FROM dbo.manufacturing_plans_current", engine)
ship   = pd.read_sql("SELECT * FROM dbo.shipments_current", engine)

# 2 · Canonicalise via approved map_attribute -------------------------------
alias = pd.read_sql("SELECT * FROM map_attribute", engine)

def apply_alias(df, attr):
    amap = alias.query("attr_type==@attr")[["customer_id","raw_value","canon_value"]]
    df = df.merge(amap,
                  left_on=["customer_id", f"{attr}_raw"],
                  right_on=["customer_id", "raw_value"],
                  how="left")
    df[f"{attr}_norm"] = df["canon_value"].fillna(df[f"{attr}_raw"]).str.upper().str.strip()
    return df.drop(columns=["raw_value","canon_value"])

for col in ["style", "colour", "po", "alt_po", "delivery_method"]:
    orders = apply_alias(orders, col)
    mfg    = apply_alias(mfg, col)
    ship   = apply_alias(ship,  col)

# 3 · Layer 0 exact join -----------------------------------------------------
keys = ["customer_id","style_norm","colour_norm",
        "po_norm","alt_po_norm","delivery_method_norm"]

layer0 = ship.merge(orders, on=keys, suffixes=("_ship","_order"))
layer0 = layer0.merge(mfg, on=keys, suffixes=("", "_mfg"))

TOL = 0.05
layer0["match_flag"] = layer0.apply(
    lambda r: "EXACT_OK" if abs(r.qty_ship - r.qty_order) <= TOL*r.qty_order
              else "EXACT_QTY_MISMATCH",
    axis=1
)

matched_ship_ids = set(layer0.shipment_id)
unmatched_ship   = ship[~ship.shipment_id.isin(matched_ship_ids)]
unmatched_ord    = orders[~orders.order_id.isin(layer0.order_id)]

# 4 · Layer 1 fuzzy ----------------------------------------------------------
indexer = rl.Index()
indexer.block(left_on="customer_id", right_on="customer_id")
pairs   = indexer.index(unmatched_ship, unmatched_ord)

compare = rl.Compare()
compare.string("style_norm",  "style_norm", method="jarowinkler",  label="style")
compare.string("colour_norm", "colour_norm", method="jarowinkler",  label="colour")
compare.string("po_norm",     "po_norm",     method="jarowinkler",  label="po")
compare.string("delivery_method_norm","delivery_method_norm",
               method="jarowinkler", label="deliv")
compare.numeric("qty_ship",   "qty_order",   method="gauss",
                offset=0, scale=TOL, label="qty")
features = compare.compute(pairs, unmatched_ship, unmatched_ord)

weights = {"style":3,"colour":2,"po":3,"deliv":1,"qty":1}
score = features.dot(pd.Series(weights)).div(sum(weights))
features["score"] = score

def classify(s):
    if s >= 0.85: return "HI_CONF"
    if s >= 0.60: return "LOW_CONF"
    return "NO_MATCH"

features["match_flag"] = features.score.apply(classify)

# 5 · Persist ----------------------------------------------------------------
full = (
    pd.concat([
        layer0[["shipment_id","order_id","match_flag"]],
        features.reset_index()[["shipment_id","order_id","match_flag","score"]]
    ])
)
write_temp(engine, full, "recon_results_tmp")

# Optionally insert HI_CONF alias suggestions into alias_review_queue here
```

*(Support functions in `utils/db.py` handle temp‑table writes.)*

---

## 8 · Kestra flow (`pipelines/flows/reconcile_orders.yaml`)

```
id: reconcile_orders
namespace: aag.prod

tasks:
  - id: recon_python
    type: io.kestra.plugin.scripts.python.Script
    script: scripts/recon.py

  - id: apply_deductions
    type: io.kestra.plugin.jdbc.Execute
    sql: sql/10_deduction/apply_deductions.sql

  - id: promote_aliases
    type: io.kestra.plugin.scripts.python.Script
    script: scripts/promote_aliases.py
```

`apply_deductions.sql` reads `recon_results_tmp`, updates `orders_current` / `manufacturing_plans_current` quantities, and closes orders where appropriate.

---

## 9 · HITL alias manager (`ui/alias_manager.py`)

*Key screens*

| Screen               | Components                                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------------------------ |
| **Queue list**       | Dataframe of `alias_review_queue` (status = PENDING).                                                        |
| **Detail / approve** | Shows raw + canonical suggestion, confidence slider, *Approve* / *Reject* buttons, editable canonical field. |
| **Bulk approve**     | Checkbox per row + “Approve Selected”.                                                                       |
| **Metrics**          | Alias adoption rate, % Layer‑0 matches, top 10 raw values awaiting approval.                                 |

The Streamlit app is served in Docker alongside the pipeline image so planners access it at `http://recon‑svc:8501`.

---

```python
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mssql+pyodbc://…")

df = pd.read_sql("SELECT TOP 200 * FROM alias_review_queue WHERE status='PENDING'", engine)
st.dataframe(df)

for idx, row in df.iterrows():
    st.write(f"Customer {row.customer_id} · {row.attr_type}")
    st.write(f"RAW  : {row.raw_value}")
    st.write(f"CANON: {row.canon_value} (suggested, conf {row.confidence:.2f})")
    col1, col2, col3 = st.columns(3)
    if col1.button("Approve", key=f"a{idx}"):
        engine.execute(
            "UPDATE alias_review_queue SET status='APPROVED', approved_by=?, approved_ts=GETDATE() WHERE queue_id=?",
            st.session_state["username"], row.queue_id)

    if col2.button("Reject", key=f"r{idx}"):
        engine.execute(
            "UPDATE alias_review_queue SET status='REJECTED', approved_by=?, approved_ts=GETDATE() WHERE queue_id=?",
            st.session_state["username"], row.queue_id)

    new_val = col3.text_input("Edit canonical value", value=row.canon_value, key=f"e{idx}")
```

## 10 · Roll‑out phases

| Phase                          | Time‑box  | What ships                                                                                                                    | KPIs                                                 |
| ------------------------------ | --------- | ----------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| **Day 1 (MVP)**                | This week | • Repo skeleton<br>• Schema SQL files<br>• `recon.py` + Kestra flow (hourly)<br>• Streamlit alias manager v0 (approve/reject) | ≥ 85 % rows land in **EXACT\_OK / HI\_CONF** buckets |
| **Phase 1 – Snapshot**         | Week 2    | • `order_list_daily_snapshot` table<br>• Kestra task `nightly_snapshot.sql`                                                   | Snapshots captured nightly                           |
| **Phase 2 – ML upgrade**       | Month 1   | • `river` online learner on pair features<br>• Auto‑weight tuning per customer                                                | ≤ 5 % LOW\_CONF after 4 weeks                        |
| **Phase 3 – Historical model** | Month 2‑3 | • Spark / Delta history pipeline<br>• LightGBM classifier (see previous spec)                                                 | Precision ≥ 0.98, Recall ≥ 0.97                      |

---

## 11 · Order of operations (runtime)

1. **Python script** `recon.py`
   ↓ writes `recon_results_tmp`
2. **SQL** `apply_deductions.sql` (in one transaction)
   – deduct shipped qty → orders & mfg
   – insert HI\_CONF alias suggestions into queue
3. **Python script** `promote_aliases.py`
   – moves queue rows with `status = APPROVED` to `map_attribute`
4. **Streamlit UI** runs persistently; planners approve queue rows anytime.
5. Nightly **snapshot** task (phase 1+) persists current order books.

---

## 12 · Future‑proofing pointers

* **Version alias tables** – add `valid_from`/`valid_to` if alias meaning changes.
* **Use Delta Lake** for snapshots once Spark cluster is online.
* **Store feature matrices** (from `recordlinkage`) in Parquet for ML audit.
* **Leverage Kestra’s state store** to skip re‑processing unchanged shipments when snapshots arrive.

---

## 13 · Initial “Definition of Done”

* [ ] Repo scaffold pushed to `github.com/AAG/aag‑recon`.
* [ ] Dev container builds and runs `pytest`.
* [ ] `make up` spins Docker‑compose with Postgres + Streamlit for local testing.
* [ ] Kestra flow deployed to non‑prod namespace; first run completes with metrics printed.
* [ ] At least **one** alias approved via UI → appears in `map_attribute` and affects next run.

---

*Ping me once the scaffolding is in place; we can pair‑review `apply_deductions.sql` or tighten the similarity weights before going to prod.*
