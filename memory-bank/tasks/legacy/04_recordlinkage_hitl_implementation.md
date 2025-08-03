# Task 04: Recordlinkage + HITL Implementation

**Status:** PENDING  
**Priority:** HIGH  
**Added:** July 30, 2025  
**Updated:** July 30, 2025  
**Estimated Time:** 2-3 weeks  

## Original Request
Implement an enhanced order-shipment matching system using recordlinkage and human-in-the-loop (HITL) review to improve match rates, handle edge cases, and build a learning system.

## Thought Process
Our current matching system has two major limitations: it relies solely on exact matching after canonicalization, and it has no mechanism to learn from human decisions. After analyzing real data from GREYSON PO 4755, we found several critical patterns that need special handling:

1. **Split Shipments**: Multiple shipments (38, 82, 78, 14, 41 units) should match to a single order (156 units)
2. **Style Variants**: Codes like LSP24K59 need to be canonicalized to LSP24K88
3. **Quantity Mismatches**: Total shipped (253) exceeds ordered (156), indicating business issues

The recordlinkage library provides a robust framework for fuzzy matching with weighted attributes. Combined with a database for canonicalization and a HITL interface, we can create a system that learns from human decisions and continuously improves.

## Definition of Done

- All code implementation tasks have a corresponding test/validation sub-task (integration testing is the default, unit tests by exception - but acceptable, the agent or developer should make this call and flag for review, e2e for end-to-end flows).
- No implementation task is marked complete until the relevant test(s) pass and explicit success criteria (acceptance criteria) are met.
- Business or user outcomes are validated with production-like data whenever feasible.
- Every task and sub-task is cross-linked to the corresponding file and test for traceability.
- All tests must pass in CI/CD prior to merging to main.
- **All business-critical paths must be covered by integration tests.**

## Implementation Plan

### Phase 1: Foundation (1 week)
1. Create database schema (5 tables)
2. Add recordlinkage to requirements.txt
3. Create database utilities (src/utils/db.py)
4. Migrate existing YAML mappings to map_attribute table

### Phase 2: Core Logic (1 week)
1. Implement `src/recon.py` with recordlinkage pipeline
2. Update alias canonicalization to use database
3. Implement style exact match requirement
4. Implement split shipment detection
5. Implement duplicate match prevention
6. Create simple SQL deduction script
7. Test with GREYSON PO 4755 data

### Phase 3: HITL Interface (1 week)
1. Implement basic Streamlit UI (approve/reject workflow)
2. Add bulk operations
3. Add metrics dashboard
4. Test end-to-end process

## Progress Tracking

**Overall Status:** Not Started - 0%

### Subtasks
| ID | Description | Status | Updated | Notes |
|----|-------------|--------|---------|-------|
| 1.1 | Create database schema (5 tables) | Not Started | | |
| 1.2 | Add recordlinkage to requirements.txt | Not Started | | |
| 1.3 | Create database utilities (src/utils/db.py) | Not Started | | |
| 1.4 | Migrate existing YAML mappings to map_attribute table | Not Started | | |
| 2.1 | Implement `src/recon.py` with recordlinkage pipeline | Not Started | | |
| 2.2 | Update alias canonicalization to use database | Not Started | | |
| 2.3 | Implement style exact match requirement | Not Started | | |
| 2.4 | Implement split shipment detection | Not Started | | |
| 2.5 | Implement duplicate match prevention | Not Started | | |
| 2.6 | Create simple SQL deduction script | Not Started | | |
| 2.7 | Test with GREYSON PO 4755 data | Not Started | | |
| 3.1 | Implement basic Streamlit UI (approve/reject workflow) | Not Started | | |
| 3.2 | Add bulk operations | Not Started | | |
| 3.3 | Add metrics dashboard | Not Started | | |
| 3.4 | Test end-to-end process | Not Started | | |

## Relevant Files

- `docs/plans/recordlinkage_implementation_plan.md` - Detailed implementation plan
- `tests/analysis/analyze_recordlinkage_potential.py` - Analysis of recordlinkage approach with real data
- `src/reports/GREYSON/daterange_2025-07-03_2025-07-03_20250729_105440_reconciliation_report.md` - Example of delivery method mismatch

## Test Coverage Mapping

| Implementation Task | Test File | Outcome Validated |
|------------------|----------|------------------|
| Database Schema Creation | tests/integration/test_db_schema.py | Table structure, constraints |
| Recordlinkage Pipeline | tests/integration/test_recordlinkage.py | Match rate improvement |
| Style Exact Match Requirement | tests/integration/test_style_matching.py | Style mismatches forced to LOW_CONF |
| Split Shipment Detection | tests/integration/test_split_shipments.py | Quantity reconciliation |
| Duplicate Match Prevention | tests/integration/test_duplicate_prevention.py | One-to-one matching enforced |
| HITL Interface | tests/e2e/test_hitl_workflow.py | Approval process |

## Progress Log
### July 30, 2025
- Task created
- Detailed implementation plan finalized in docs/plans/recordlinkage_implementation_plan.md
- Analysis of real data confirmed viability of approach
