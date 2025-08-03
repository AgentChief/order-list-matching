# [Task 04] - Recordlinkage + HITL Implementation

**Status:** In Progress  
**Added:** July 30, 2025  
**Updated:** July 30, 2025

## Original Request
Implement a robust order-shipment matching system using the recordlinkage library combined with human-in-the-loop (HITL) review for uncertain matches.

## Thought Process
The current matching system has limitations:
1. Exact matching only handles ~70% of cases (115/165)
2. Fuzzy matching is not working effectively (0/50)
3. No mechanism for human review of uncertain matches
4. Configuration is static in YAML files, difficult to update

The recordlinkage library offers:
- Multiple comparison methods (exact, string similarity, numeric)
- Configurable weighting of different fields
- Score-based classification
- Efficient processing of large datasets

Adding HITL review allows:
- Expert judgment for uncertain matches
- Learning from past decisions
- Dynamic updating of mapping rules
- Continuous improvement of match rates

Database-backed configuration provides:
- Dynamic updates via HITL interface
- Centralized storage
- Transaction support
- Complex queries and lookups

## Definition of Done
- Database schema implemented with 5 core tables
- Recordlinkage library integrated and configured
- Core matching logic implemented with confidence levels
- HITL review interface created with Streamlit
- Value mappings migrated from YAML to database
- Match rate improved to >90% for test dataset (GREYSON PO 4755)

## Implementation Plan
1. Create database schema
   - map_attribute: Stores canonical mappings for attribute values
   - customer_attribute_config: Customer-specific matching configurations
   - attribute_equivalence_rule: Rules for field comparisons
   - alias_review_queue: Queue of uncertain matches for HITL review
   - alias_review_history: History of past HITL decisions

2. Integrate recordlinkage library
   - Add to requirements.txt
   - Create core matching module (src/core/match_recordlinkage.py)
   - Implement comparison functions for each field type

3. Implement confidence-based classification
   - HIGH_CONF: Direct use in reconciliation (>0.9)
   - MED_CONF: Warning flag but used (0.7-0.9)
   - LOW_CONF: Sent to HITL review (<0.7)

4. Create HITL review interface
   - Streamlit app for reviewing uncertain matches
   - Accept/reject/modify functionality
   - Update mapping rules based on decisions

5. Migrate value mappings
   - Extract existing YAML mappings
   - Insert into map_attribute table
   - Update code to use database instead of YAML

## Progress Tracking

**Overall Status:** In Progress - 0%

### Subtasks
| ID | Description | Status | Updated | Notes |
|----|-------------|--------|---------|-------|
| 1.1 | Create database schema | Not Started | July 30, 2025 | Need to design 5 tables |
| 1.2 | Add recordlinkage to requirements | Not Started | July 30, 2025 | Also need pandas-dedupe |
| 1.3 | Create database utilities | Not Started | July 30, 2025 | For managing configs and mapping |
| 1.4 | Implement core matching logic | Not Started | July 30, 2025 | With confidence levels |
| 1.5 | Build HITL interface | Not Started | July 30, 2025 | Using Streamlit |
| 1.6 | Migrate YAML mappings to database | Not Started | July 30, 2025 | Extract and insert |

## Relevant Files

- `src/core/match_recordlinkage.py` - To be created for recordlinkage integration
- `src/utils/db.py` - To be created for database utilities
- `src/app/streamlit_app.py` - To be created for HITL interface
- `config/canonical_customers.yaml` - Current mappings to migrate
- `config/value_mappings.yaml` - Current mappings to migrate

## Test Coverage Mapping

| Implementation Task                | Test File                               | Outcome Validated                    |
|------------------------------------|----------------------------------------|------------------------------------|
| Database schema                    | tests/test_db_schema.py                | Tables created and accessible       |
| Recordlinkage integration          | tests/test_match_recordlinkage.py      | Matching with confidence levels     |
| HITL interface                     | Manual testing                         | Review workflow functions           |
| YAML to database migration         | tests/test_migration.py                | Mappings correctly transferred      |

## Progress Log

### July 30, 2025
- Created task file and detailed implementation plan
- Analyzed sample data to identify key matching fields
- Researched recordlinkage library capabilities
- Drafted database schema design
