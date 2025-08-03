# TASK005 - Architecture Realignment (Critical Assessment)

**Status:** Pending  
**Added:** July 31, 2025  
**Updated:** July 31, 2025  
**Priority:** CRITICAL

## Original Request
Following a brutal review of the current development state against the two original plans (build_notes.md and build_notes_alternate.md), we need to realign the architecture to eliminate hybrid approaches and focus on a single, scalable solution.

## Thought Process

### Critical Assessment Summary
The current development has created a **hybrid mess** that combines the worst of both original plans:
- **Plan A** (build_notes.md): Simple, lean file-based pipeline with pandas → rapidfuzz → LM Studio
- **Plan B** (build_notes_alternate.md): Enterprise SQL schema with recordlinkage, HITL, and proper audit trails

**Current State Problems:**
1. **Duplicate matching logic**: Both rapidfuzz (src/core/) AND recordlinkage (reconciliation/) exist
2. **No clear path**: SQL staging layer but no HITL, no snapshots, no proper workflow
3. **Brittle orchestration**: Python orchestrator duplicates what proper workflow tools would do
4. **Mixed naming conventions**: FACT_ORDER_LIST vs snake_case staging tables
5. **No CI/testing**: Migration scripts fail, no automated validation
6. **Memory bank drift**: Documentation doesn't match actual development

### Recommended Direction
**Choose Plan B fully** - Enterprise approach with proper HITL and audit trails. Reasons:
- Matches real-world reconciliation requirements
- Supports organizational scale and audit needs
- Eliminates CSV bottlenecks
- Clear path to production readiness

## Definition of Done

- [x] Architectural decision made and documented
- [ ] Codebase pruned to single approach
- [ ] Schema finalized with consistent naming
- [ ] HITL workflow implemented (Streamlit)
- [ ] CI/CD pipeline established
- [ ] Memory bank updated to reflect new direction
- [ ] All existing tasks re-evaluated against new architecture

## Implementation Plan

### Phase 1: Codebase Hygiene (Week 1)
1. **Delete Plan A implementation**
   - Remove src/core/* (rapidfuzz + LM Studio modules)
   - Delete build_notes.md implementation references
   - Keep only recordlinkage_matcher.py as sole matcher

2. **Fix immediate blocker**
   - Complete the missing create_orders_extended_table function in db_orchestrator.py
   - Get current SQL approach working end-to-end
   - Test with GREYSON PO 4755 data

3. **Schema finalization**
   - Lock naming conventions (choose snake_case or UPPER, stick to it)
   - Run clean migration in fresh dev DB
   - Stop runtime ALTER TABLE patches

### Phase 2: Core Functionality (Week 2)
4. **Recordlinkage integration**
   - Update recordlinkage_matcher to read from new FACT tables only
   - Remove dependency on CSV files
   - Test end-to-end matching pipeline

5. **Snapshot system**
   - Add incremental snapshot table (order_list_daily_snapshot)
   - Create nightly job for historical tracking

### Phase 3: HITL Implementation (Week 3)
6. **Streamlit HITL interface**
   - Build minimal page to list alias_review_queue
   - Implement approve/reject functionality
   - Write-back to map_attribute system

### Phase 4: Production Readiness (Week 4)
7. **CI/CD Pipeline**
   - Add pytest + sqlfluff tests
   - GitHub Action: run migrations + tests on PR
   - Fail build if migration breaks

8. **Enhanced Python orchestrator**
   - Add proper error handling and logging
   - Implement retry logic and better status reporting
   - Keep as orchestration tool (defer Kestra/Airflow to future)

## DECISION MADE: PLAN B APPROVED
**Date:** July 31, 2025  
**Decision:** Implement Plan B (enterprise SQL + HITL) fully, eliminate Plan A code  
**Orchestration:** Keep Python db_orchestrator.py for now, defer Kestra/DAG to later phase  

## Progress Tracking

**Overall Status:** In Progress - 5% Complete

### Subtasks
| ID | Description | Status | Updated | Notes |
|----|-------------|--------|---------|-------|
| 5.1 | Delete Plan A code (src/core/*) | Not Started | | Remove rapidfuzz/LLM paths |
| 5.2 | Fix db_orchestrator.py missing function | Not Started | | **IMMEDIATE**: Complete create_orders_extended_table |
| 5.3 | Schema naming convention lockdown | Not Started | | Choose and apply consistently |
| 5.4 | Clean database migration | Not Started | | Fresh dev DB setup |
| 5.5 | Update recordlinkage_matcher | Not Started | | Remove CSV dependencies |
| 5.6 | Add snapshot system | Not Started | | Daily order_list_snapshot |
| 5.7 | Build Streamlit HITL interface | Not Started | | Alias management UI |
| 5.8 | Implement CI/CD pipeline | Not Started | | GitHub Actions + tests |
| 5.9 | Enhance Python orchestrator | Not Started | | Keep as orchestration tool |
| 5.10 | Update memory bank documentation | Not Started | | Reflect new architecture |

## Relevant Files

- `docs/build_notes.md` - Plan A (to be deprecated)
- `docs/build_notes_alternate.md` - Plan B (to be implemented fully)
- `src/core/` - Directory to be deleted
- `src/reconciliation/recordlinkage_matcher.py` - Keep as sole matcher
- `db_orchestrator.py` - Fix immediately, replace later
- Memory bank files - All need updating

## Test Coverage Mapping

| Phase | Test Type | Coverage Needed |
|-------|-----------|----------------|
| Phase 1 | Unit | Database schema validation |
| Phase 2 | Integration | End-to-end matching pipeline |
| Phase 3 | UI | Streamlit HITL workflows |
| Phase 4 | System | Full CI/CD pipeline |

## Progress Log

### July 31, 2025
- Created task following brutal architectural review
- Identified critical problems with current hybrid approach
- Developed 4-phase implementation plan
- Set priority as CRITICAL due to architectural drift
- **DECISION APPROVED**: Plan B (enterprise SQL + HITL) selected
- **ORCHESTRATION DECISION**: Keep Python db_orchestrator.py, defer Kestra/DAG to future
- **IMMEDIATE NEXT**: Fix missing create_orders_extended_table function to unblock current work

## Key Risks
1. **Time pressure**: Temptation to continue hybrid approach rather than proper cleanup
2. **Sunk cost fallacy**: Resistance to deleting existing code
3. **Scope creep**: Adding features before core architecture is solid
4. **Database dependencies**: Current SQL scripts have bugs that need fixing first

## Success Criteria
- Single, clear architectural approach
- Working end-to-end pipeline from DB to HITL
- Proper CI/CD preventing future regressions
- Documentation that matches reality
- Scalable foundation for enterprise deployment
