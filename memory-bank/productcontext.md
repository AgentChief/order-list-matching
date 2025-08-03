# Product Context

## Why This Project Exists
The Order-List Matching System addresses a critical business problem: reconciling orders and shipments across various data sources and formats. Our business partners need to know if their orders are being fulfilled correctly, but differences in terminology, data formats, and split shipments make this challenging. The system automatically matches orders to their corresponding shipments, provides visibility into match rates, and identifies patterns that require human review.

## Problems It Solves
1. **Data Format Inconsistency**: Orders and shipments often use different terminology (e.g., "SEA-FB" vs "FAST BOAT") for the same concept
2. **Split Shipments**: A single order might be fulfilled through multiple shipments, complicating quantity matching
3. **Style Variations**: Minor differences in style codes (LSP24K59 vs LSP24K88) can cause mismatches
4. **Manual Reconciliation Time**: Without automation, reconciliation requires extensive manual work
5. **No Learning Mechanism**: Current systems don't learn from human decisions, repeating the same matching problems

## How It Should Work
The system follows a multi-layered approach:

1. **Layer 0 - Exact Matching**: Match orders and shipments based on exact values (after canonicalization)
2. **Layer 1 - Fuzzy Matching**: Use recordlinkage library for fuzzy matching with business rules
3. **Human-in-the-Loop (HITL)**: Present low-confidence matches for human review
4. **Learning System**: Store approved mappings in database for future matching

Each match is classified as:
- **HI_CONF**: High confidence match (≥0.85 similarity)
- **LOW_CONF**: Low confidence match (0.60-0.85 similarity)
- **NO_MATCH**: No suitable match found (<0.60 similarity)

## User Experience Goals
1. **Operators**: Quickly review and approve/reject suggested mappings through a simple Streamlit interface
2. **Analysts**: Access reconciliation reports showing match rates and patterns
3. **Business Partners**: Receive accurate reconciliation data with complete visibility into order fulfillment
4. **Developers**: Easily extend the system with new matching rules and attributes through configuration

## Key Metrics
- **Exact Match Rate**: Target >90% after initial HITL cycles
- **Overall Match Rate**: Target >95% including fuzzy matches
- **Human Review Time**: Reduce by >50% compared to manual reconciliation
- **Pattern Recognition**: Identify and canonicalize common variations automatically
