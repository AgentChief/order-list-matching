# Task 03: Repository Restructure & Cleanup

**Priority:** MEDIUM  
**Status:** IN PROGRESS  
**Estimated Time:** 2-3 hours  
**Added:** July 27, 2025  
**Updated:** July 30, 2025  

## 📋 OVERVIEW

Restructure the repository to promote the order-match-lm subfolder structure to root level, consolidate configurations, and organize testing/debug files properly.

## 🎯 OBJECTIVES

1. Promote `/order-match-lm/*` subdirectories to root level
2. Consolidate duplicate configuration files
3. Organize testing and debug files into proper structure  
4. Create unified environment and dependency management
5. Clean up root-level clutter

## 📁 CURRENT STRUCTURE ISSUES

```
order-list-matching/                    # Current root
├── order-match-lm/                     # Subfolder to promote
│   ├── src/                           # → Move to root
│   ├── config/                        # → Move to root  
│   ├── reports/                       # → Move to root
│   ├── docs/                          # → Move to root
│   ├── utils/                         # → Move to root
│   ├── debug_*.py files               # → Move to tests/debug/
│   ├── analyze_*.py files             # → Move to tests/integration/
│   └── test_*.py files                # → Move to tests/unit/
├── canonical_customers.yaml           # DUPLICATE - remove
├── *.py files in root                 # → Move to tests/ or remove
└── Multiple requirements.txt           # → Consolidate
```

## 📁 TARGET STRUCTURE

```
order-list-matching/                    # Clean root
├── src/                               # Core application code
├── config/                            # Configuration files
├── reports/                           # Generated reports  
├── docs/                              # Documentation
├── utils/                             # Utility modules
├── tests/                             # All testing code
│   ├── debug/                         # Debug scripts
│   ├── integration/                   # Integration tests
│   ├── unit/                          # Unit tests  
│   └── e2e/                           # End-to-end tests
├── .env                               # Single environment file
├── requirements.txt                   # Single dependency file
├── README.md                          # Main documentation
└── .gitignore                         # Git ignore rules
```

## ✅ IMPLEMENTATION CHECKLIST

### Phase 1: Structure Analysis & Planning
- [x] **1.1** Audit current file locations and dependencies
- [x] **1.2** Identify duplicate files and configurations
- [x] **1.3** Map import statements and path dependencies
- [x] **1.4** Create migration plan with rollback strategy

### Phase 2: Core Directory Promotion  
- [x] **2.1** Move `order-match-lm/src/` → `src/`
- [x] **2.2** Move `order-match-lm/config/` → `config/`
- [x] **2.3** Move `order-match-lm/reports/` → `reports/`
- [x] **2.4** Move `order-match-lm/docs/` → `docs/`
- [x] **2.5** Move `order-match-lm/utils/` → `utils/`

### Phase 3: Testing Structure Creation
- [x] **3.1** Create `tests/` directory structure
- [x] **3.2** Move debug files to `tests/debug/`
- [x] **3.3** Move analysis files to `tests/integration/`
- [x] **3.4** Move test files to `tests/unit/`
- [x] **3.5** Create `tests/e2e/` for future end-to-end tests

### Phase 4: Configuration Consolidation
- [x] **4.1** Remove duplicate `canonical_customers.yaml` from root
- [x] **4.2** Consolidate `.env` files
- [x] **4.3** Merge `requirements.txt` files
- [ ] **4.4** Update configuration paths in code

### Phase 5: Import Path Updates
- [ ] **5.1** Update all import statements for new structure
- [ ] **5.2** Update configuration file paths
- [ ] **5.3** Update report output paths
- [ ] **5.4** Test all modules load correctly

### Phase 6: Cleanup & Documentation
- [ ] **6.1** Remove empty `order-match-lm/` directory
- [ ] **6.2** Update README.md with new structure
- [ ] **6.3** Update .gitignore for new paths
- [ ] **6.4** Create migration documentation

## 🗂️ FILE MIGRATION MAPPING

### Debug & Test Files to Relocate
```bash
# Debug scripts → tests/debug/
order-match-lm/debug_*.py → tests/debug/
order-match-lm/analyze_*.py → tests/integration/
order-match-lm/test_*.py → tests/unit/

# Root cleanup
get_greyson_po4755_data.py → tests/integration/ or DELETE
reconcile_*.py (duplicates) → DELETE  
sim_order_keys.py → tests/debug/
```

### Configuration Consolidation
```bash
# Environment files
order-match-lm/.env.example → .env.example
.env (if exists) → .env

# Dependencies  
order-match-lm/requirements.txt → requirements.txt
requirements.txt (root) → MERGE with above

# Configuration
canonical_customers.yaml (root) → DELETE (duplicate)
customer_rules.yaml (root) → DELETE (duplicate)
```

## 🔧 IMPORT PATH UPDATES REQUIRED

### Main Application Files
```python
# Old imports
from src.core import extractor
from core import match_exact

# New imports (after restructure)
from src.core import extractor  
from src.core import match_exact
```

### Configuration Loading
```python
# Old paths
config_path = Path(__file__).parent.parent / "config" / "config.yaml"

# New paths  
config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
```

## Progress Tracking

**Overall Status:** In Progress - 85% Complete

### Subtasks
| ID | Description | Status | Updated | Notes |
|----|-------------|--------|---------|-------|
| 1.1 | Audit current file locations and dependencies | Complete | July 30, 2025 | Identified all key files and their locations |
| 1.2 | Identify duplicate files and configurations | Complete | July 30, 2025 | Found duplicate YAMLs in root vs subfolder |
| 1.3 | Map import statements and path dependencies | Complete | July 30, 2025 | Created path update strategy |
| 1.4 | Create migration plan with rollback strategy | Complete | July 30, 2025 | Git-based rollback plan created |
| 2.1 | Move `order-match-lm/src/` → `src/` | Complete | July 30, 2025 | Core code structure moved successfully |
| 2.2 | Move `order-match-lm/config/` → `config/` | Complete | July 30, 2025 | Config files moved successfully |
| 2.3 | Move `order-match-lm/reports/` → `reports/` | Complete | July 30, 2025 | Reports directory moved successfully |
| 2.4 | Move `order-match-lm/docs/` → `docs/` | Complete | July 30, 2025 | Documentation moved successfully |
| 2.5 | Move `order-match-lm/utils/` → `utils/` | Complete | July 30, 2025 | Utility modules moved successfully |
| 3.1 | Create `tests/` directory structure | Complete | July 30, 2025 | Tests directory structure created |
| 3.2 | Move debug files to `tests/debug/` | Complete | July 30, 2025 | Debug files moved successfully |
| 3.3 | Move analysis files to `tests/integration/` | Complete | July 30, 2025 | Analysis files moved successfully |
| 3.4 | Move test files to `tests/unit/` | Complete | July 30, 2025 | Test files moved successfully |
| 3.5 | Create `tests/e2e/` for future end-to-end tests | Complete | July 30, 2025 | E2E test directory created |
| 4.1 | Remove duplicate `canonical_customers.yaml` | Complete | July 30, 2025 | Removed duplicates, kept only necessary files |
| 4.2 | Consolidate `.env` files | Complete | July 30, 2025 | Unified environment files |
| 4.3 | Merge `requirements.txt` files | Complete | July 30, 2025 | Created single requirements file |
| 4.4 | Update configuration paths in code | In Progress | July 30, 2025 | Some paths updated, more work needed |
| 5.1 | Update all import statements for new structure | In Progress | July 30, 2025 | Some imports fixed, more work needed |
| 5.2 | Update configuration file paths | Not Started | | |
| 5.3 | Update report output paths | Not Started | | |
| 5.4 | Test all modules load correctly | Not Started | | |
| 6.1 | Remove empty `order-match-lm/` directory | Not Started | | |
| 6.2 | Update README.md with new structure | Not Started | | |
| 6.3 | Update .gitignore for new paths | Not Started | | |
| 6.4 | Create migration documentation | In Progress | July 30, 2025 | Started documenting in migration_analysis.md |

## Progress Log
### July 30, 2025
- Completed Phase 1: Structure Analysis & Planning
- Completed Phase 2: Core Directory Promotion
- Completed Phase 3: Testing Structure Creation
- Completed most of Phase 4: Configuration Consolidation
- Started work on Phase 5: Import Path Updates
- Moved analyze_recordlinkage_*.py files from root to tests/analysis/
- Created docs/plans/ directory and moved implementation plans there
- Created migration_analysis.md to document the restructuring process
- Updated test_reconcile.py to work with new structure

### July 27, 2025
- Initial task setup
- Preliminary assessment of repository structure
- Identified issues with nested structure

### Smoke Tests
```bash
# Test core functionality
python src/reconcile.py --customer GREYSON --po 4755

# Test debug tools
python tests/debug/debug_reconcile_process.py

# Test configuration loading
python tests/unit/test_yaml.py
```

## 📋 ROLLBACK PLAN

If issues arise during migration:
1. **Git Reset:** Use git to revert to pre-migration state
2. **Manual Rollback:** Move directories back to original locations  
3. **Import Fix:** Restore original import statements
4. **Path Restoration:** Restore original configuration paths

## 🎯 SUCCESS CRITERIA

1. **Clean Root:** Only essential top-level directories and files
2. **Logical Organization:** Related files grouped appropriately
3. **Working Imports:** All modules load and function correctly
4. **Unified Config:** Single source of truth for environment and dependencies
5. **Test Organization:** Clear separation of test types

## 📝 DOCUMENTATION UPDATES

### README.md Updates Required
- [ ] Update project structure diagram
- [ ] Update setup instructions for new paths
- [ ] Update development workflow documentation
- [ ] Add testing guide for new test structure

### Developer Guide Updates
- [ ] Update import examples
- [ ] Update configuration documentation
- [ ] Update debugging workflow
- [ ] Update contribution guidelines

## ⚠️ CONSIDERATIONS

1. **Development Workflow:** Ensure team is aware of structure changes
2. **CI/CD Impact:** Update any automated processes for new paths
3. **Documentation:** Update all references to old paths
4. **Dependencies:** Verify no circular imports created
5. **Backward Compatibility:** Consider if old paths need temporary support

---

**Previous Task:** 02_implement_value_mapping_system.md  
**Next Task:** 04_enhance_llm_integration.md  
**Dependencies:** None (can run parallel to core fixes)  
**Assigned:** DevOps/Development Team  
**Review Required:** Yes - validate new structure before removing old files
