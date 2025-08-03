# Project Brief

## Overview
The **Order-List Matching System** is designed to automatically reconcile orders and shipments across various data sources and formats. It addresses the critical business need to verify that customer orders are being fulfilled correctly despite differences in terminology, data formats, and split shipments. The system uses a layered matching approach with human-in-the-loop (HITL) review for continuous improvement.

## Core Requirements and Goals

- **Multi-Layered Matching Strategy:** Implement a two-layered approach with Layer 0 (exact matching after canonicalization) and Layer 1 (fuzzy matching with recordlinkage) to maximize match rates while ensuring accuracy.

- **Human-in-the-Loop Learning:** Create a system that learns from human decisions through a review interface for low-confidence matches, gradually improving match rates over time.

- **Database-Driven Configuration:** Move from static YAML files to a dynamic database-driven approach that enables real-time updates, centralized storage, and transaction support.

- **Split Shipment Detection:** Intelligently handle cases where multiple shipments fulfill a single order by consolidating shipments by style+color+PO before comparing quantities.

- **Style Exact Match Requirement:** Enforce the business rule that styles must match exactly (similarity = 1.0) or be forced to human review regardless of overall match score.

- **Robust Canonicalization:** Build a comprehensive system to normalize variations in terminology (e.g., "SEA-FB" vs "FAST BOAT") through a centralized mapping table.

- **Customer-Specific Configurations:** Support both global and customer-specific attribute mappings, weights, and match requirements.

## Project Scope

This project covers the end-to-end reconciliation of orders and shipments, including:

1. **Data Loading & Normalization:** Loading order and shipment data from source systems and normalizing formats
2. **Canonicalization:** Applying known mappings to standardize terminology
3. **Exact Matching:** Matching records based on canonical values
4. **Fuzzy Matching:** Using recordlinkage for intelligent similarity-based matching
5. **HITL Review:** Human approval/rejection of suggested mappings
6. **Learning System:** Storing approved mappings for future matching
7. **Reporting:** Generating reconciliation reports with match rates and patterns

Out of scope for this project are:
- Data extraction from source systems (assumed to be handled separately)
- Financial reconciliation beyond order-shipment matching
- Real-time matching (system runs as a batch process)

## Success Criteria

This project will be considered successful when:

- **Match Rate Improvement:** Exact match rate improves from current ~70% to 90%+ after initial HITL cycles
- **Overall Match Rate:** Total match rate (exact + approved fuzzy) reaches 95%+
- **Human Review Efficiency:** Manual reconciliation time is reduced by at least 50%
- **Split Shipment Handling:** System correctly identifies and handles split shipments
- **Style Accuracy:** No style mismatches occur without human review
- **Learning Capability:** System demonstrably improves match rates over time as mappings accumulate
- **Comprehensive Testing:** All business-critical paths covered by integration tests
- **Maintainability:** Clean, documented code with proper structure and database schema
