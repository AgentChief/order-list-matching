# Order List Matching

AI-powered order reconciliation system using LM Studio for intelligent matching between orders and shipments.

## Features

- **3-Stage Matching Pipeline**: Exact → Fuzzy → LLM-based matching
- **Database-to-Database**: Pure DB reconciliation, no CSV dependencies  
- **AI-Powered**: Uses LM Studio for complex matching scenarios
- **Configurable**: Customer-specific rules and canonical mappings

## Quick Start

### 1. Configure
* `config/config.yaml` – fill real SQL Server strings.
* LM Studio running on `localhost:1234` with the model name in config.

### 2. Bootstrap
```bash
setup_repo.bat        # Windows
# or
bash setup_repo.bat   # WSL / Linux / macOS
```

### 3. Run once

```bash
.venv\Scripts\activate
python src/reconcile.py --customer GREYSON --po 4755 --use-llm
```

Outputs:

* `/reports/validation/GREYSON/4755_YYYYMMDD_matches.csv`
* `/reports/validation/GREYSON/4755_YYYYMMDD_unmatched.csv` (if any)

## Project Structure

```
order-list-matching/
├─ config/                       # Configuration files
│   ├─ config.yaml              # Database credentials & settings
│   ├─ canonical_customers.yaml # Customer name mappings
│   └─ value_mappings.yaml      # Value mappings for reconciliation
├─ src/                         # Source code
│   ├─ reconcile.py            # Main CLI entry point
│   ├─ llm_client.py           # LM Studio API interface
│   ├─ llm_analysis_client_batched.py # Batch analysis of reconciliation results
│   └─ core/                   # Core matching modules
│       ├─ extractor.py        # Database extraction
│       ├─ normalise.py        # Data normalization
│       ├─ match_exact.py      # Exact matching logic
│       ├─ match_fuzzy.py      # Fuzzy matching logic
│       └─ match_llm.py        # LLM-based matching
├─ utils/                       # Utility modules
│   └─ db_helper.py            # Database connectivity utilities
├─ tests/                       # Test directory
├─ docs/                        # Documentation files
└─ reports/                     # Auto-generated reports
```

## Configuration

### Database Setup (`config/config.yaml`)
```yaml
databases:
  orders:
    host: your-server
    port: 1433
    database: ORDERS
    username: username
    password: password
    encrypt: yes
    trustServerCertificate: yes
  shipments:
    host: your-server
    port: 1433
    database: WMS
    username: username
    password: password
    encrypt: yes
    trustServerCertificate: yes

llm:
  url: "http://localhost:1234/v1/chat/completions"
  model: "mixtral-8x7b-instruct"

report_root: "reports"
```

### Customer Mappings (`config/canonical_customers.yaml`)
```yaml
customers:
  - canonical: "GREYSON"
    aliases:
      - "Greyson"
      - "Greyson Clothiers"
      - "GREYSON CLOTHIERS"
    map:
      "PO NUMBER": "Customer_PO"
      "CUSTOMER STYLE": "Style"
      "CUSTOMER COLOUR DESCRIPTION": "Color"
      "PLANNED DELIVERY METHOD": "Shipping_Method"
    fuzzy_threshold: 85
```

## CLI Usage

```bash
# Basic reconciliation
python src/reconcile.py --customer GREYSON --po 4755

# With fuzzy matching threshold (via config file)
python src/reconcile.py --customer GREYSON --po 4755

# Enable LLM for complex matches
python src/reconcile.py --customer GREYSON --po 4755 --use-llm

# Date range reconciliation
python src/reconcile.py --customer GREYSON --date-from 2023-01-01 --date-to 2023-01-31

# Generate individual reports per shipment date
python src/reconcile.py --customer GREYSON --date-from 2023-01-01 --date-to 2023-01-31 --by-date

# Process all customers with recent shipments
python src/reconcile.py --date-from 2023-01-01 --date-to 2023-01-31
```

## Dependencies

- **ruamel.yaml**: Configuration file parsing
- **pandas**: Data manipulation and analysis
- **pyodbc**: SQL Server database connectivity
- **rapidfuzz**: Fast fuzzy string matching
- **requests**: HTTP requests to LM Studio API

## Requirements

- Python 3.8+
- SQL Server ODBC drivers
- LM Studio (for AI-powered matching)
- Access to orders and shipments databases
