
# Database Operations

This directory contains all database schemas, migrations, models, and documentation for the Order List Matching system.

## Directory Structure (2025+ Standard)

```
db/
├── ddl/                # Data Definition Language (CREATE TABLE, etc.)
│   ├── data_model/     # Business tables (post-reconciliation)
│   └── reconciliation/ # Reconciliation process tables
├── models/             # dbt-style SQL models (staging → intermediate → marts)
│   ├── staging/
│   ├── intermediate/
│   └── marts/
│       └── reconciliation/
├── migrations/         # Timestamped migration scripts
├── procedures/         # Stored procedures
├── schema/             # Current schema definitions
├── scripts/            # Utility scripts
├── tests/              # Database tests
├── docs/               # Database-specific documentation
│   ├── schema/
│   ├── migrations/
│   └── performance/
└── examples/           # Usage examples
```

## Documentation Standards
- All business and technical documentation: `docs/`
- Database-specific documentation: `db/docs/`
- AI memory and task tracking: `memory-bank/`

## Naming Conventions
- Use `snake_case` for all new database objects
- Use `lowercase_with_underscores` for all documentation and code files (except TASK files)
- Staging models: `stg_`, Intermediate: `int_`, Marts: `mart_`, Fact: `fact_`, Dim: `dim_`, Views: `v_`, Tests: `test_`

## dbt-Style Model Layers
- **Staging**: Minimal transformations, standardization
- **Intermediate**: Business logic, calculated fields
- **Marts**: Business-facing, final presentation

## Schema Evolution
- Migrations tracked in `migrations/` with `YYYYMMDD_HHMMSS_description.sql`
- Each migration script should be idempotent and include error handling

## Review & Maintenance
- Review documentation and structure quarterly or after major refactors
- Remove obsolete files and folders promptly
