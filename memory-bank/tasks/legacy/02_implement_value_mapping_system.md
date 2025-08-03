# Task 02: Implement Value Mapping System

**Priority:** HIGH  
**Status:** PENDING  
**Estimated Time:** 4-6 hours  

## 📋 OVERVIEW

Implement comprehensive value mapping to handle the 40 unmatched shipments by addressing delivery method mismatches, style variants, and color formatting differences.

## 🎯 OBJECTIVES

1. Create delivery method mapping rules for `AIR/SEA SPLIT` ↔ `SEA-FB`
2. Implement style variant handling for `-1`, `-2` suffixes
3. Add color normalization for space differences
4. Improve match rate from 75.8% to 85%+ through intelligent mapping

## 📊 TARGET IMPROVEMENTS

**Current State (Post-Fix):**
- Total: 165 shipments
- Exact matches: 125 (75.8%)
- Unmatched: 40 (24.2%)

**Target State:**
- Total: 165 shipments  
- Exact + Mapped matches: 140+ (85%+)
- Unmatched: 25- (15%-)

## 🧩 KNOWN MISMATCH PATTERNS

### 1. Delivery Method Mismatches
```yaml
# Orders vs Shipments
"AIR/SEA SPLIT" → "SEA-FB"  # Primary mismatch
"AIR" → "AIR"               # ✅ Match
"SEA" → "SEA"               # ✅ Match
```

### 2. Style Variant Patterns  
```yaml
# Orders (with variants) vs Shipments (base)
"LFA25K30A-1" → "LFA25K30A"  # Strip variant
"LFA25K30A-2" → "LFA25K30A"  # Strip variant
"LSP24K59-1" → "LSP24K59"   # Strip variant
"LSP24K59-2" → "LSP24K59"   # Strip variant
```

### 3. Color Formatting Issues
```yaml
# Orders vs Shipments  
"234 - ARCTIC / WOLF BLUE" → "234 - ARCTIC/WOLF BLUE"  # Space normalization
```

## ✅ IMPLEMENTATION CHECKLIST

### Phase 1: Delivery Method Mapping
- [ ] **1.1** Update `config/value_mappings.yaml` with delivery method rules
- [ ] **1.2** Implement delivery method pre-processing in normalization
- [ ] **1.3** Test delivery method mapping with PO 4755
- [ ] **1.4** Validate business logic with stakeholders

### Phase 2: Style Variant Handling
- [ ] **2.1** Create style variant detection regex patterns
- [ ] **2.2** Implement base style extraction logic
- [ ] **2.3** Add fuzzy matching for style variants (confidence: 0.9)
- [ ] **2.4** Test with known variant patterns

### Phase 3: Color Normalization
- [ ] **3.1** Implement color text normalization function
- [ ] **3.2** Handle space/punctuation differences
- [ ] **3.3** Add to pre-processing pipeline
- [ ] **3.4** Test color matching improvements

### Phase 4: Integration & Testing
- [ ] **4.1** Integrate all mappings into reconciliation pipeline
- [ ] **4.2** Test end-to-end with PO 4755
- [ ] **4.3** Validate improved match rates
- [ ] **4.4** Document mapping rules for business review

## 🔧 TECHNICAL IMPLEMENTATION

### Value Mapping Configuration
```yaml
# config/value_mappings.yaml
PLANNED_DELIVERY_METHOD:
  global_mappings:
    "SEA-FB": "AIR/SEA SPLIT"
    "FAST BOAT": "AIR/SEA SPLIT"
  confidence: 1.0

CUSTOMER_STYLE:
  fuzzy_patterns:
    - pattern: "^(.+)-[0-9]+$"
      replacement: "$1"
      confidence: 0.9
      description: "Strip numeric variant suffix"

CUSTOMER_COLOUR_DESCRIPTION:
  normalization_rules:
    - type: "whitespace"
      action: "normalize"
      confidence: 1.0
```

### Pre-processing Pipeline
```python
def preprocess_data(orders, shipments, cfg):
    # 1. Apply delivery method mappings
    orders = apply_delivery_method_mapping(orders, cfg)
    shipments = apply_delivery_method_mapping(shipments, cfg)
    
    # 2. Normalize style variants
    orders = normalize_style_variants(orders, cfg)
    shipments = normalize_style_variants(shipments, cfg)
    
    # 3. Normalize color descriptions
    orders = normalize_colors(orders, cfg)
    shipments = normalize_colors(shipments, cfg)
    
    return orders, shipments
```

## 📋 VALIDATION TESTS

### Test Case 1: Delivery Method Mapping
```python
# Input
order_method = "AIR/SEA SPLIT"
ship_method = "SEA-FB"

# Expected After Mapping
assert map_delivery_method(ship_method) == order_method
```

### Test Case 2: Style Variant Handling
```python
# Input
order_styles = ["LFA25K30A-1", "LFA25K30A-2"]
ship_style = "LFA25K30A"

# Expected After Mapping
for style in order_styles:
    assert normalize_style_variant(style) == ship_style
```

### Test Case 3: Color Normalization
```python
# Input
order_color = "234 - ARCTIC / WOLF BLUE"
ship_color = "234 - ARCTIC/WOLF BLUE"

# Expected After Mapping
assert normalize_color(order_color) == normalize_color(ship_color)
```

## 🧪 BUSINESS VALIDATION REQUIRED

### Delivery Method Mappings
- [ ] Confirm `SEA-FB` = `AIR/SEA SPLIT` is correct business logic
- [ ] Validate any other delivery method aliases
- [ ] Review impact on shipping cost calculations

### Style Variant Rules
- [ ] Confirm variant stripping is appropriate for matching
- [ ] Validate that `-1`, `-2` suffixes are indeed variants
- [ ] Review impact on inventory tracking

### Color Matching Rules
- [ ] Confirm space normalization doesn't affect color identification
- [ ] Validate punctuation handling rules
- [ ] Review impact on color-specific reporting

## 📊 SUCCESS METRICS

**Delivery Method Improvements:**
- Current unmatched due to delivery methods: ~15 shipments
- Target: Reduce to 0 unmatched delivery methods

**Style Variant Improvements:**
- Current unmatched due to style variants: ~20 shipments  
- Target: Reduce to 5 unmatched styles (after mapping)

**Color Normalization Improvements:**
- Current unmatched due to color formatting: ~5 shipments
- Target: Reduce to 0 unmatched colors

## 🔍 MONITORING & REPORTING

Add to reconciliation reports:
1. **Mapping Statistics:** Show how many records used each mapping rule
2. **Confidence Tracking:** Report confidence levels for fuzzy matches
3. **Business Impact:** Show improvement in match rates
4. **Exception Reporting:** List remaining unmatched items with reasons

## 🚨 RISK CONSIDERATIONS

1. **Business Logic Validation:** Ensure mappings don't create false matches
2. **Data Integrity:** Maintain audit trail of all mappings applied
3. **Performance Impact:** Monitor processing time with additional rules
4. **Rollback Capability:** Ability to disable mappings if issues arise

---

**Previous Task:** 01_fix_core_matching_system.md  
**Next Task:** 03_enhance_reporting_system.md  
**Dependencies:** Task 01 completion  
**Assigned:** Development Team  
**Review Required:** Yes - business stakeholder approval for mapping rules
