# Advanced LLM Matching Analysis System

## Overview
Enhance the current LLM analysis to provide intelligent matching suggestions for low-confidence or unmatched records, with human-in-the-loop (HITL) approval.

## Current State
- ✅ Basic LLM analysis working with DeepSeek model
- ✅ Markdown report generation
- ✅ Integration with reconciliation pipeline
- ✅ Structured data input (grouped by PO, properly contextualized)

## Proposed Enhanced Features

### 1. Structured Data Grouping & Batching
**Current Issue**: Data sent to LLM is random samples
**Solution**: Group and sort data strategically

#### Implementation:
- **By Date**: Group shipments/cartons by shipping date for temporal analysis
- **By PO Number**: Analyze all shipments for a specific PO together
- **By Customer**: Customer-specific patterns and rules
- **Batch Processing**: Send data in logical chunks to LLM

```python
# Example structure
shipment_batches = {
    "2025-07-03": {
        "PO_4909": [list_of_shipments],
        "PO_4755": [list_of_shipments]
    }
}
```

### 2. Multi-Dimensional Matching Analysis
**Goal**: Systematic analysis across different dimensions

#### Analysis Types:
1. **Style Analysis**: All styles for a customer, identify patterns
2. **Color Analysis**: Color description variations and mappings
3. **PO Analysis**: PO number formatting patterns
4. **Shipping Method Analysis**: Delivery method variations

#### LLM Input Structure:
```json
{
    "analysis_type": "style_patterns",
    "customer": "GREYSON",
    "data": {
        "unmatched_styles": ["MFA24K80", "MFA25K56"],
        "order_styles": ["MCLSCB25", "TSHIRT001"],
        "fuzzy_matches": [
            {"shipment": "MFA24K80", "order": "MFA24-K80", "confidence": 85}
        ]
    }
}
```

### 3. Intelligent Matching Dictionary with HITL
**Goal**: LLM suggests mappings, human approves/rejects

#### Workflow:
1. **LLM Analysis**: Identify potential matches below 90% confidence
2. **Pattern Recognition**: LLM suggests mapping rules
3. **HITL Review**: Present suggestions to user for approval
4. **Learning**: Store approved mappings for future use

#### Example LLM Output:
```json
{
    "suggested_mappings": [
        {
            "type": "style_mapping",
            "from": "MFA24K80",
            "to": "MFA24-K80",
            "confidence": 85,
            "rationale": "Similar alphanumeric pattern, likely formatting difference",
            "requires_approval": true
        },
        {
            "type": "shipping_method",
            "from": "SEA-FB",
            "to": "SEA FAST BOAT",
            "confidence": 92,
            "rationale": "Common abbreviation pattern",
            "requires_approval": false
        }
    ]
}
```

### 4. Implementation Phases

#### Phase 1: Enhanced Data Structure (Current Priority)
- ✅ Fix current prompt to show correct matching context
- ✅ Group data by PO and date
- ✅ Show orders that match shipment PO numbers
- ✅ Explain 4-field matching criteria

#### Phase 2: Batch Analysis System
- [ ] Implement date-based batching
- [ ] Add PO-grouped analysis
- [ ] Create customer-specific analysis runs
- [ ] Add analysis type selection (style, color, PO, shipping)

#### Phase 3: Pattern Recognition & Suggestions
- [ ] Enhance LLM prompts for pattern identification
- [ ] Implement structured output for suggested mappings
- [ ] Add confidence scoring for suggestions
- [ ] Create mapping suggestion database

#### Phase 4: HITL Interface
- [ ] Build web interface for reviewing suggestions
- [ ] Implement approval/rejection workflow
- [ ] Store approved mappings in customer patterns
- [ ] Auto-apply high-confidence mappings

#### Phase 5: Learning & Automation
- [ ] Track approval patterns
- [ ] Improve LLM suggestions based on feedback
- [ ] Implement automatic rule generation
- [ ] Add performance metrics and reporting

### 5. Technical Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Reconcile     │───▶│  LLM Analysis    │───▶│  HITL Interface │
│   Pipeline      │    │  Client          │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Structured     │    │  Pattern         │    │  Approved       │
│  Data Batches   │    │  Recognition     │    │  Mappings DB    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### 6. Configuration Extensions

```yaml
# config/config.yaml
llm_analysis:
  url: "http://localhost:1234/v1/chat/completions"
  model: "deepseek/deepseek-r1-0528-qwen3-8b"
  temperature: 0.3
  max_tokens: 3000
  
  # New configurations
  batch_processing:
    enabled: true
    batch_size: 50  # records per batch
    group_by: ["date", "po_number"]
  
  pattern_analysis:
    enabled: true
    confidence_threshold: 90  # below this, suggest mappings
    auto_apply_threshold: 95  # above this, auto-apply
    
  hitl:
    enabled: true
    require_approval: true
    suggestion_limit: 10  # max suggestions per analysis
```

### 7. Expected Benefits

1. **Improved Matching Accuracy**: Better context leads to better suggestions
2. **Reduced Manual Work**: Automate common pattern recognition
3. **Learning System**: Continuously improve based on human feedback
4. **Scalability**: Handle large datasets efficiently with batching
5. **Customer-Specific Intelligence**: Build custom rules per customer

### 8. Success Metrics

- **Match Rate Improvement**: Target 10-20% increase in automatic matches
- **Manual Review Efficiency**: Reduce manual review time by 50%
- **Pattern Recognition Accuracy**: 90%+ approval rate for LLM suggestions
- **Processing Speed**: Handle 1000+ records in batches efficiently

---

## Next Steps (Immediate)

1. **Fix Current System**: Ensure proper data context in LLM prompts
2. **Test Enhanced Analysis**: Verify LLM understands matching criteria
3. **Plan Phase 2**: Design batch processing architecture
4. **Prototype HITL Interface**: Simple web form for approving mappings

---

*This document outlines the roadmap for transforming the current LLM analysis into an intelligent matching assistance system with human oversight.*
