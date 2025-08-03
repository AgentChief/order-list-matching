# Database Operations

This directory contains database schemas, migrations, and queries for the Order List Matching system.

## Directory Structure

```
db/
├─ ddl/                   # Data Definition Language (CREATE TABLE statements)
│  ├─ reconciliation/     # Tables for reconciliation and validation processes
│  └─ data_model/         # Tables for data model staging (post-reconciliation)
├─ migrations/            # Database migration scripts
│  ├─ reconciliation/     # Migrations for reconciliation tables
│  └─ data_model/         # Migrations for data model tables
├─ queries/               # SQL query files
│  ├─ reconciliation/     # Queries for reconciliation processes
│  └─ data_model/         # Queries for data model operations
├─ models/                # dbt-style SQL models (new structure)
│  ├─ staging/            # Raw data with minimal transformations
│  ├─ intermediate/       # Business logic transformations
│  └─ marts/              # Business-facing presentation layer
│     └─ reconciliation/  # Domain-specific models for reconciliation
├─ procedures/            # Stored procedures for data synchronization
└─ tests/                 # Data quality tests
```

### Transitioning to dbt-Style Structure
We are transitioning toward a dbt-style layered architecture for better maintainability:

```
db/
├─ models/                # SQL model definitions
│  ├─ staging/            # Raw data with minimal transformations
│  ├─ intermediate/       # Business logic transformations
│  └─ marts/              # Business-facing presentation layer
│     └─ reconciliation/  # Domain-specific models for reconciliation
├─ procedures/            # Stored procedures for data synchronization
├─ tests/                 # Data quality tests
└─ migrations/            # Database migration scripts
```

## Reconciliation vs Data Model

### Reconciliation Tables
Tables in the reconciliation schema are used for:
- Matching and validating orders against shipments
- Storing attribute mappings and configurations
- Managing human-in-the-loop (HITL) review processes
- Tracking alias and variant resolution

These tables support the operational process of reconciling orders and shipments.

### Data Model Tables
Tables in the data model schema are used for:
- Storing reconciled and validated data
- Serving as a source for reporting and analytics
- Supporting downstream business processes
- Maintaining a clean, integrated view of orders and shipments

These tables contain data that has passed through the reconciliation process and is considered valid for business use.

## dbt-Style Model Layers

Our new structure follows dbt-style layered architecture:

### Staging Layer
Contains views that perform minimal transformations, primarily:
- Renaming fields to follow standard naming conventions
- Filtering out deleted records
- Type casting
- Handling NULL values

Key staging models:
- `stg_fm_orders_shipped.sql`: Standardizes shipment data fields
- `stg_order_list.sql`: Standardizes order data fields from FACT_ORDER_LIST

### Intermediate Layer
Contains transformations that implement business logic:
- Extending data with calculated fields
- Joining related tables
- Preparing data for final presentation

Key intermediate models:
- `int_orders_extended.sql`: Extends order data with calculated fields
- `int_shipments_extended.sql`: Extends shipment data with calculated fields

### Marts Layer
Business-facing presentation layer:
- Aggregates data for specific business domains
- Implements final business logic
- Provides consistent, well-documented interfaces

Key mart models:
- `mart_fact_order_list.sql`: Final presentation of order data
- `mart_fact_orders_shipped.sql`: Final presentation of shipment data
- `mart_reconciliation_summary.sql`: Order-shipment reconciliation results

## Schema Evolution

Database migrations are tracked in the `migrations/` directory with timestamped files following the naming convention:
```
YYYYMMDD_HHMMSS_description.sql
```

Each migration script should be idempotent where possible and include appropriate error handling.

## Naming Conventions

- All new object names use snake_case
- Staging models are prefixed with `stg_`
- Intermediate models are prefixed with `int_`
- Mart models are prefixed with `mart_`
- Fact tables use the `fact_` prefix in their object names
- Dimension tables use the `dim_` prefix in their object names
- Views use the `v_` prefix in legacy code (transitioning to dbt-style naming)
- Tests use the `test_` prefix
