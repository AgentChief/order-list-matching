# Technical Context

## Technologies Used

### Core Technologies
- **Python**: Primary development language
- **pandas**: Data manipulation and analysis
- **recordlinkage**: Python library for record linkage and fuzzy matching
- **SQL Server**: Database for storing mappings and configurations
- **Streamlit**: UI for human-in-the-loop review

### Supporting Libraries
- **pyodbc**: SQL Server connectivity
- **rapidfuzz**: Optional enhancement for fuzzy string matching
- **SQLAlchemy**: ORM for database operations

## Development Setup
- **Environment**: Windows with Python 3.10+
- **Database**: SQL Server with tables for attribute mapping and review queue
- **Configuration**: Transitioning from YAML to database-driven approach
- **Version Control**: Git with feature branch workflow

## Technical Constraints
- **Performance**: Must handle thousands of order-shipment comparisons efficiently
- **Memory Usage**: Large datasets must be processed without exhausting memory
- **Style Requirements**: Styles must match exactly, or be forced to human review
- **Quantity Matching**: Must handle split shipments (multiple shipments for one order)

## Dependencies
- **External Services**: None for core functionality
- **Database Schema**: 
  - `map_attribute`: Source of truth for canonicalization
  - `customer_attribute_config`: Customer-specific attribute settings
  - `alias_review_queue`: HITL review workflow
  - `recon_results_tmp`: Reconciliation results storage

## Infrastructure
- **Deployment**: Local Python application with database connection
- **Scheduled Execution**: Daily reconciliation runs
- **Monitoring**: Logging to application logs and database

## Technical Debt
- **Legacy YAML Configuration**: Being migrated to database
- **Repository Structure**: Recently restructured, some import paths may need updates
- **Testing Coverage**: Integration tests needed for recordlinkage approach
