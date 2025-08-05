# System Patterns

## System Architecture

The Order-List Matching System follows a layered architecture with a human-in-the-loop component:

```mermaid
flowchart TD
    subgraph Input
        Orders[Order Data]
        Shipments[Shipment Data]
    end
    
    subgraph Preprocessing
        Normalize[Normalize Data]
        Canon[Canonicalize Values]
    end
    
    subgraph Matching
        L0[Layer 0: Exact Matching]
        L1[Layer 1: Fuzzy Matching]
    end
    
    subgraph Classification
        HI[HI_CONF Matches]
        LOW[LOW_CONF Matches]
        NO[NO_MATCH]
    end
    
    subgraph HITL
        Review[Human Review Queue]
        Approve[Approve/Reject]
    end
    
    subgraph Database
        MapAttr[map_attribute]
        CustConfig[customer_attribute_config]
        ReviewQueue[alias_review_queue]
        Results[recon_results_tmp]
    end
    
    Orders --> Normalize
    Shipments --> Normalize
    Normalize --> Canon
    Canon --> L0
    L0 --> HI
    L0 --> L1
    L1 --> HI
    L1 --> LOW
    L1 --> NO
    LOW --> Review
    Review --> Approve
    Approve --> MapAttr
    MapAttr --> Canon
    CustConfig --> L0
    CustConfig --> L1
    Review --> ReviewQueue
    HI --> Results
    LOW --> Results
    NO --> Results
```

## Database & Schema Organization

### Database Folder Structure (`db/`)
```
db/                           # Primary database assets
├── ddl/                      # Data Definition Language (CREATE statements)
│   ├── data_model/           # Business tables (post-reconciliation)
│   │   ├── fact_order_list.sql
│   │   ├── fact_orders_shipped.sql
│   │   └── stored_procedures/
│   └── reconciliation/       # Reconciliation process tables
│       ├── reconciliation_result.sql
│       ├── match_attribute_score.sql
│       └── hitl_queue.sql
├── models/                   # dbt-style SQL models (NEW ARCHITECTURE)
│   ├── staging/              # Raw data + minimal transformations
│   │   ├── stg_order_list.sql
│   │   └── stg_fm_orders_shipped.sql
│   ├── intermediate/         # Business logic transformations
│   │   ├── int_orders_extended.sql
│   │   └── int_shipments_extended.sql
│   └── marts/                # Business-facing presentation layer
│       └── reconciliation/
│           ├── mart_fact_order_list.sql
│           ├── mart_fact_orders_shipped.sql
│           └── mart_reconciliation_summary.sql
├── migrations/               # Database migration scripts (timestamped)
│   ├── 01_create_stg_fm_orders_shipped_table.sql
│   ├── 05_create_reconciliation_tables.sql
│   └── 20230925_120000_dbt_structure_migration.sql
├── procedures/               # Stored procedures
│   ├── sp_refresh_shipment_summary_cache.sql
│   └── sync_fm_orders_to_fact.sql
├── schema/                   # Current schema definitions
│   ├── shipment_summary_cache.sql
│   └── config_schema.sql
├── scripts/                  # Utility scripts
├── tests/                    # Database tests
├── docs/                     # Database-specific documentation
├── examples/                 # Usage examples
└── queries/                  # Ad-hoc queries (currently empty)
    ├── data_model/           # (empty - reserved for ad-hoc queries)
    └── reconciliation/       # (empty - reserved for ad-hoc queries)
```

### Legacy Issues to Resolve
- **`sql/` root folder**: Contains `hitl_tables.sql` → **MOVE to `db/ddl/reconciliation/`**
- **`database/` folder**: 34 duplicate files → **CONSOLIDATE into `db/` (TASK014)**
- **Empty `queries/` subfolders**: Document purpose or remove

### dbt-Style Architecture Evolution
**CURRENT STATE**: Transitioning from legacy DDL to dbt-style layered models
**TARGET STATE**: Full dbt-style with staging → intermediate → marts flow

**Data Flow**: `SOURCE TABLES → STAGING → INTERMEDIATE → MARTS → MOVEMENT TABLE`
1. ✅ **Source**: FACT_ORDER_LIST, FM_orders_shipped
2. ✅ **Staging**: stg_order_list, stg_fm_orders_shipped  
3. ✅ **Intermediate**: int_orders_extended, int_shipments_extended
4. ✅ **Marts**: mart_fact_order_list, mart_fact_orders_shipped
5. ✅ **Performance Layer**: shipment_summary_cache (TASK001)
6. 🆕 **Future**: fact_order_movements (TASK013)

## Order of Operations
SOURCE TABLES → STAGING → INTERMEDIATE → MARTS → MOVEMENT TABLE
     ✅              ✅            ✅         ✅         🔶 READY

1. ✅ FACT_ORDER_LIST (orders source)
2. ✅ FM_orders_shipped (shipments source)  
3. ✅ stg_order_list → int_orders_extended → mart_fact_order_list
4. ✅ stg_fm_orders_shipped → int_shipments_extended → mart_fact_orders_shipped
5. ✅ reconciliation_result (match metadata)
6. ✅ shipment_summary_cache (TASK001 performance layer)
7. 🆕 fact_order_movements (needs creation - TASK013)

## Key Technical Decisions

1. **Database-Driven Approach**: Moving from YAML to database for configuration and mappings to enable:
   - Real-time updates via HITL
   - Centralized storage
   - Transaction support
   - Complex queries

2. **recordlinkage Library**: Selected for fuzzy matching due to:
   - Comprehensive similarity metrics
   - Configurable comparators
   - Scalable indexing

3. **Layered Matching Strategy**:
   - Layer 0 (Exact): Fast and deterministic for known patterns
   - Layer 1 (Fuzzy): Handles variations with confidence scores
   - HITL: Human review for borderline cases

4. **Style Exact Match Requirement**: Styles must match exactly or be forced to LOW_CONF regardless of overall score to prevent incorrect matches

5. **Split Shipment Detection**: Pre-process to consolidate shipments by style+color+PO before quantity comparison

## Design Patterns in Use

1. **Repository Pattern**: Data access abstracted through repository classes
2. **Factory Method**: For creating different matching strategies
3. **Strategy Pattern**: Swappable matching algorithms
4. **Observer Pattern**: For monitoring match results
5. **Command Pattern**: For HITL operations

## Component Relationships

### Data Flow
1. Orders and shipments are loaded from source systems
2. Data is normalized (date formats, capitalization, etc.)
3. Values are canonicalized using map_attribute table
4. Layer 0 attempts exact matching on canonical values
5. Unmatched records proceed to Layer 1 for fuzzy matching
6. Matches are classified by confidence level
7. LOW_CONF matches are queued for human review
8. Approved mappings are added to map_attribute table
9. Results are stored in reconciliation_result table
10. Reconciled shipments are marked in FM_orders_shipped with a reconciliation_status flag
11. Split shipments are linked using a common split_group_id

### Post-Reconciliation Data Updates
1. **Shipment Status Updates**: After reconciliation pipeline runs, the system automatically updates the data model's shipped orders tables:
   - Sets reconciliation_status to 'MATCHED' for high-confidence and approved matches
   - Sets reconciliation_status to 'UNMATCHED' for confirmed non-matches
   - Sets reconciliation_status to 'PENDING_REVIEW' for matches in HITL queue
   - Records reconciliation_date timestamp for audit trail
   - Stores reconciliation_id for linking back to detailed match information
   - Updates order_id reference to link shipment with corresponding order record
   - Updates order_line_id reference when specific size/line item is matched

2. **Exception Management Workflow**:
   - Unmatched shipments are flagged in data model with reconciliation_status = 'UNMATCHED'
   - Records requiring manual intervention are added to hitl_queue table with priority score
   - The HITL review process uses Streamlit for visualization and decision-making
   - Streamlit interface CANNOT directly edit source data tables - it can only:
     - Approve/reject suggested matches
     - Select alternative order matches from search results
     - Add mappings/aliases to be used in future matching
     - Add notes explaining decision rationale
   - All HITL decisions generate entries in reconciliation_audit_log
   - After HITL review, an automated process applies approved changes to data model tables
   - Weekly exception reports highlight patterns requiring business process improvements

3. **Split Shipment Handling**:
   - Split shipments detected based on quantity/line item distribution
   - Related shipments linked via split_group_id field in the data model
   - Primary shipment record identified with is_primary_shipment flag
   - Child shipments reference parent via parent_shipment_id
   - Each split gets appropriate portion of quantity distributed
   - HITL interface provides special split shipment view for easier validation

### Key Components
- **Core Matching Engine**: Orchestrates the matching process
- **Canonicalization Service**: Applies known mappings from database
- **HITL Interface**: Streamlit UI for human review and exception management
- **Database Layer**: Persistence for mappings, results, and reconciliation status
- **Reporting Module**: Generates reconciliation reports and exception summaries
- **Status Tracking Service**: Updates data_model tables with reconciliation status

## Data Flow Between Reconciliation and Data Model

```mermaid
flowchart TD
    subgraph ReconciliationProcess
        MatchEngine[Matching Engine]
        ReconciliationTables[Reconciliation Tables]
        HITL[HITL Review Interface]
    end
    
    subgraph DataModel
        ShippedOrders[Shipped Orders Table]
        OrderLines[Order Lines Table]
        ReconciliationMetrics[Reconciliation Metrics]
    end
    
    MatchEngine -->|Stores results| ReconciliationTables
    ReconciliationTables -->|Flags for review| HITL
    HITL -->|Updates decisions| ReconciliationTables
    
    ReconciliationTables -->|Updates status & links| ShippedOrders
    ReconciliationTables -->|Updates match references| OrderLines
    ReconciliationTables -->|Aggregates metrics| ReconciliationMetrics
    
    ShippedOrders -->|Provides current state| MatchEngine
    OrderLines -->|Provides line items| MatchEngine
```

This flow ensures that the reconciliation process maintains the integrity of the data model while providing the necessary feedback loops for continuous improvement.
