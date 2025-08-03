# Task 01: Fix Core Matching System

**Priority:** CRITICAL  
**Status:** PENDING  
**Estimated Time:** 2-3 hours  

## 📋 OVERVIEW

Fix the fundamental issue where 40 out of 165 shipments are silently ignored, causing false 100% match rates when actual rate is 75.8%.

## 🎯 OBJECTIVES

1. Fix `match_exact.py` leftover calculation logic
2. Ensure all shipments are accounted for (matches + unmatched = total)
3. Provide accurate reporting of unmatched shipments
4. Restore true match rate visibility

## 📊 CURRENT STATE vs TARGET

**Before Fix:**
- Reported: 125/125 matches (100% - FALSE)
- Reality: 125/165 matches (75.8% - TRUE)
- Unmatched: 0 reported (40 hidden)

**After Fix Target:**
- Total shipments: 165 (accurate)
- Exact matches: 125 (honest reporting)
- Unmatched: 40 (visible with reasons)
- True match rate: 75.8% (accurate)

## 🔧 ROOT CAUSE ANALYSIS

**Issue in `src/core/match_exact.py`:**
```python
# PROBLEMATIC CODE:
ships = ships.rename(columns={v: k for k, v in cfg["map"].items()})  # Renames ships
m = orders.merge(ships, on=join_cols, how="inner", suffixes=("_o", "_s"))  # Uses renamed ships
leftover = ships[~ships[original_ship_id_col].isin(matched_ship_ids)]  # BUT checks original ships!
```

**Problems:**
1. Join uses RENAMED ships dataframe
2. Leftover calculation uses ORIGINAL ships dataframe  
3. This creates inconsistency - matched shipments aren't properly identified
4. Result: 40 shipments disappear from tracking

## ✅ IMPLEMENTATION CHECKLIST

### Phase 1: Fix Core Logic
- [ ] **1.1** Backup current `src/core/match_exact.py`
- [ ] **1.2** Fix dataframe consistency in leftover calculation
- [ ] **1.3** Ensure original shipment ID column preservation
- [ ] **1.4** Add validation: total_ships = matches + unmatched

### Phase 2: Testing & Validation
- [ ] **2.1** Test with PO 4755: verify 165 total, 125 matches, 40 unmatched
- [ ] **2.2** Test with July 2025 dataset: verify no shipments disappear
- [ ] **2.3** Run `debug_reconcile_process.py` to confirm fix
- [ ] **2.4** Validate against historical data

### Phase 3: Reporting Updates
- [ ] **3.1** Update reconciliation reports to show true totals
- [ ] **3.2** Add unmatched shipment analysis
- [ ] **3.3** Include match rate calculation in output
- [ ] **3.4** Generate sample reports for validation

## 🧪 TEST CASES

**Test Case 1: PO 4755**
- Input: 69 orders, 165 shipments
- Expected: 125 matches, 40 unmatched, 0 missing
- Validation: 125 + 40 = 165 ✓

**Test Case 2: July 2025 Full Month**
- Input: Multiple POs, 217 shipments
- Expected: All shipments accounted for in matches/unmatched
- Validation: No "disappearing" shipments

## 📋 DETAILED MISMATCH ANALYSIS

**Known Data Quality Issues to Surface:**

1. **Delivery Method Mismatches (Priority High)**
   - Orders: `AIR/SEA SPLIT` vs Ships: `SEA-FB`
   - Need value mapping rules

2. **Style Variant Mismatches (Priority Medium)**  
   - Orders: `LFA25K30A-1`, `LFA25K30A-2` vs Ships: `LFA25K30A`
   - 28 order styles have no shipment equivalents

3. **Color Formatting Issues (Priority Low)**
   - Orders: `234 - ARCTIC / WOLF BLUE` vs Ships: `234 - ARCTIC/WOLF BLUE`
   - Space differences breaking exact matches

## 🔍 VALIDATION COMMANDS

```bash
# Before fix:
python debug_reconcile_process.py
# Should show: 165 → 125 → 0 (40 missing)

# After fix:
python debug_reconcile_process.py  
# Should show: 165 → 125 → 40 (all accounted)

# Full test:
python src/reconcile.py --customer GREYSON --po 4755
# Should report: 125/165 matches (75.8%), 40 unmatched
```

## 🚨 CRITICAL SUCCESS CRITERIA

1. **No Missing Shipments:** Total input = matches + unmatched
2. **Accurate Reporting:** True match rates displayed
3. **Unmatched Visibility:** All non-matching shipments reported with reasons
4. **Data Integrity:** No silent failures or hidden records

## 📝 IMPLEMENTATION NOTES

- **Configuration Status:** ✅ CORRECT (GREYSON uses proper 4-column matching)
- **Join Columns:** `PO NUMBER`, `PLANNED DELIVERY METHOD`, `CUSTOMER STYLE`, `CUSTOMER COLOUR DESCRIPTION`
- **Global Fallbacks:** Working correctly
- **Historical Reports:** Archive before changes

## 🔄 ROLLBACK PLAN

If issues arise:
1. Restore backed up `match_exact.py`
2. Revert to previous reporting format
3. Document issues for future fix iteration

---

**Next Task:** 02_implement_value_mapping_system.md  
**Dependencies:** None  
**Assigned:** Development Team  
**Review Required:** Yes - validate with stakeholders that lower match rates are acceptable for accuracy
