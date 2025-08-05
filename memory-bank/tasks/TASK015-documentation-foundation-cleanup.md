# TASK015 - Documentation Foundation Cleanup

**Status:** Critical Priority  
**Added:** August 5, 2025  
**Updated:** August 5, 2025  
**Priority:** CRITICAL 🚨 (BLOCKING TASK014 & TASK013)

## Original Request
Establish proper documentation foundation and folder structure, consolidate scattered documentation, implement naming conventions (lowercase_underscore), and migrate plans from memory-bank to appropriate documentation folders. This cleanup provides the foundation for TASK014 and TASK013.

## Thought Process

### Critical Foundation Issues
**DISCOVERED DURING TASK013 PLANNING:**
- **Scattered documentation**: Plans in `memory-bank/plans/` should be in `docs/plans/`
- **Mixed naming conventions**: Capital letters, spaces, inconsistent separators throughout
- **Unclear hierarchy**: Documentation spread across `docs/`, `db/docs/`, `memory-bank/`
- **SQL files misplaced**: Root `sql/` folder content should be in `db/ddl/`
- **Empty placeholder folders**: `db/queries/` subfolders are empty and unused

### Documentation Strategy
Establish clear documentation hierarchy:
```
docs/                          # Primary documentation
├── architecture/              # System architecture and design docs
├── analysis/                 # Business analysis and findings
├── plans/                    # Project plans and roadmaps
├── api/                      # API documentation
├── user_guide/               # User documentation
├── deployment/               # Deployment and operations
└── standards/                # Coding and documentation standards

db/docs/                      # Database-specific documentation only
├── schema/                   # Database schema documentation
├── migrations/               # Migration documentation
└── performance/              # Database performance docs

memory-bank/                  # AI context only - no business docs
├── tasks/                    # Task tracking
├── activeContext.md          # Current work context
├── progress.md               # Project progress
└── (core AI context files)
```

### Naming Convention Standard
**ALL files and folders**: `lowercase_with_underscores`
- ✅ Good: `task013_movement_table_plan.md`
- ❌ Bad: `TASK013-consolidated-movement-table-implementation.md`
- ❌ Bad: `Task 13 Movement Table.md`

## Definition of Done

- [ ] Documentation hierarchy established with proper folder structure
- [ ] All files renamed to lowercase_underscore convention
- [ ] Plans migrated from `memory-bank/plans/` to `docs/plans/`
- [ ] Root `sql/` folder consolidated into `db/ddl/` structure
- [ ] Empty `db/queries/` subfolders removed or documented purpose
- [ ] Documentation standards documented in `docs/standards/`
- [ ] Clear separation between AI memory-bank and business documentation
- [ ] All documentation paths updated in code references
- [ ] README files updated to reflect new structure

## Implementation Plan

### Step 1: Establish Documentation Structure
```powershell
# Create standardized docs folder structure
New-Item -Path "docs/architecture" -ItemType Directory -Force
New-Item -Path "docs/analysis" -ItemType Directory -Force  
New-Item -Path "docs/plans" -ItemType Directory -Force
New-Item -Path "docs/api" -ItemType Directory -Force
New-Item -Path "docs/user_guide" -ItemType Directory -Force
New-Item -Path "docs/deployment" -ItemType Directory -Force
New-Item -Path "docs/standards" -ItemType Directory -Force

# Database docs specialization
New-Item -Path "db/docs/schema" -ItemType Directory -Force
New-Item -Path "db/docs/migrations" -ItemType Directory -Force
New-Item -Path "db/docs/performance" -ItemType Directory -Force
```

### Step 2: Migrate Plans from Memory Bank
```powershell
# Move plans to proper location
Move-Item "memory-bank/plans/*" "docs/plans/"

# Keep only AI-specific content in memory-bank
# Business plans, roadmaps, etc. belong in docs/
```

### Step 3: Implement Naming Convention
Rename all files to lowercase_underscore:
- `TASK013-consolidated-movement-table-implementation.md` → `task013_consolidated_movement_table_implementation.md`
- `MASTER_IMPLEMENTATION_PLAN.md` → `master_implementation_plan.md`
- `data_flow_diagram.md` → Already compliant ✅

### Step 4: Consolidate SQL Files
```powershell
# Move misplaced SQL files to proper db structure
Move-Item "sql/hitl_tables.sql" "db/ddl/reconciliation/"

# Remove empty sql/ folder
Remove-Item "sql/" -Force
```

### Step 5: Clean Empty Folders
```powershell
# Document purpose or remove empty query folders
# Currently: db/queries/data_model/ and db/queries/reconciliation/ are empty
```

### Step 6: Update Documentation Standards
Create `docs/standards/documentation_standards.md`:
- File naming conventions
- Folder organization rules
- Documentation hierarchy
- AI memory-bank vs business documentation separation

### Step 7: Update References
```powershell
# Search and update all file references
Select-String -Path "**/*.py" -Pattern "memory-bank/plans"
Select-String -Path "**/*.md" -Pattern "memory-bank/plans"
Select-String -Path "**/*.sql" -Pattern "sql/"

# Update to new paths
```

## Progress Tracking

**Overall Status:** Critical Priority - 0% Complete

### Subtasks
| ID | Description | Status | Updated | Notes |
|----|-------------|--------|---------|-------|
| 1.1 | Create standardized docs folder structure | Complete | Aug 5 | ✅ Created 7 docs + 3 db/docs folders |
| 1.2 | Migrate plans from memory-bank to docs/plans | Complete | Aug 5 | ✅ Moved 7 plan files, removed empty folder |
| 1.3 | Rename all files to lowercase_underscore | Skipped for TASK files | Aug 5 | Only business/plan/docs renamed |
| 1.4 | Consolidate sql/ folder into db/ddl/ | Complete | Aug 5 | ✅ hitl_tables.sql moved, sql/ removed |
| 1.5 | Remove or document empty db/queries folders | Complete | Aug 5 | ✅ Removed empty db/queries folders |
| 1.6 | Create documentation standards file | Complete | Aug 5 | ✅ Created docs/standards/documentation_standards.md |
| 1.7 | Update all code references to new paths | Not Started | Aug 5 | Python, SQL, Markdown files |
| 1.8 | Validation testing of updated references | Not Started | Aug 5 | Ensure no broken links |

## Relevant Files

- `docs/` - Target documentation structure
- `memory-bank/plans/` - Plans to migrate  
- `sql/hitl_tables.sql` - File to consolidate
- `db/queries/` - Empty folders to evaluate
- **ALL .md files** - Need naming convention updates
- **ALL code files** - May contain path references to update

## Progress Log
### August 5, 2025 - 11:09 AM
- ✅ **Step 1 COMPLETE**: Created standardized documentation structure
  - docs/: architecture, analysis, plans, api, user_guide, deployment, standards
  - db/docs/: schema, migrations, performance
- ✅ **Step 2 COMPLETE**: Migrated all business plans from memory-bank to docs/plans, removed empty folder
- ✅ **Step 3 COMPLETE**: Renamed all business/plan/docs files to lowercase_underscore (TASK files remain capitalized)
- ✅ **Step 4 COMPLETE**: Moved hitl_tables.sql to db/ddl/reconciliation/, removed sql/ folder
- ✅ **Step 5 COMPLETE**: Removed empty db/queries folders
- ✅ **Step 6 COMPLETE**: Created docs/standards/documentation_standards.md
- Updated db/README.md and docs/README_new.md to reflect new standards
  - All folder and naming conventions now documented
  - Ready for reference update and validation

## Dependencies
- **BLOCKS**: TASK014 (database consolidation needs clean documentation structure)  
- **BLOCKS**: TASK013 (movement table needs proper documentation foundation)
- **ENABLES**: All future documentation and project organization

## Impact
**High Impact**: Provides clean foundation for all subsequent work
- Eliminates confusion between AI memory-bank and business documentation
- Standardizes naming conventions across entire project
- Creates proper hierarchy for documentation discovery and maintenance
- Enables confident execution of TASK014 database consolidation
