"""
HITL (Human-in-the-Loop) review component for order-shipment matching.
"""
import streamlit as st
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import logging
from pathlib import Path
import json
from datetime import datetime

from src.utils.db import (
    execute_query,
    execute_non_query,
    execute_stored_procedure,
    get_customer_match_config,
    get_connection
)

# Add auth_helper for database connection
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from auth_helper import get_connection_string
import pyodbc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def execute_query_with_auth(query: str, params: list = None):
    """Execute query using auth_helper connection"""
    try:
        with pyodbc.connect(get_connection_string()) as conn:
            df = pd.read_sql(query, conn, params=params or [])
            return df.to_dict('records') if not df.empty else []
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return []


def get_pending_reviews(assigned_to: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get pending reviews from the HITL queue.
    
    Args:
        assigned_to: Filter by assigned user (or None for unassigned)
        limit: Maximum number of records to return
        
    Returns:
        List of dictionaries with review data
    """
    query = """
    SELECT TOP (?)
        q.id AS queue_id,
        q.reconciliation_id,
        q.priority,
        q.status,
        q.assigned_to,
        r.customer_name,
        r.po_number,
        r.match_status,
        r.confidence_score,
        r.match_method,
        r.order_id,
        r.shipment_id,
        r.match_details,
        r.created_at
    FROM 
        hitl_queue q
    JOIN 
        reconciliation_result r ON q.reconciliation_id = r.id
    WHERE 
        q.status IN ('pending', 'in_review')
    """
    
    params = [limit]
    
    if assigned_to is not None:
        query += " AND (q.assigned_to = ? OR q.assigned_to IS NULL)"
        params.append(assigned_to)
    else:
        query += " AND q.assigned_to IS NULL"
    
    query += " ORDER BY q.priority DESC, r.created_at ASC"
    
    try:
        return execute_query(query, params)
    except Exception as e:
        logger.error(f"Error getting pending reviews: {e}")
        return []


def claim_review(queue_id: int, user: str) -> bool:
    """
    Claim a review for processing.
    
    Args:
        queue_id: ID in hitl_queue table
        user: Username claiming the review
        
    Returns:
        True if successful, False otherwise
    """
    query = """
    UPDATE hitl_queue
    SET 
        status = 'in_review',
        assigned_to = ?,
        review_started_at = GETDATE(),
        updated_at = GETDATE()
    WHERE 
        id = ? AND 
        (status = 'pending' OR (status = 'in_review' AND assigned_to = ?))
    """
    
    try:
        rows_affected = execute_non_query(query, [user, queue_id, user])
        return rows_affected > 0
    except Exception as e:
        logger.error(f"Error claiming review: {e}")
        return False


def submit_review(
    queue_id: int,
    user: str,
    decision: str,
    reason: str,
    match_order_id: Optional[int] = None
) -> bool:
    """
    Submit a review decision.
    
    Args:
        queue_id: ID in hitl_queue table
        user: Username submitting the review
        decision: 'approve', 'reject', 'need_more_info'
        reason: Reason for the decision
        match_order_id: Order ID to match with (for manual matches)
        
    Returns:
        True if successful, False otherwise
    """
    # First update the HITL queue
    hitl_query = """
    UPDATE hitl_queue
    SET 
        status = 'reviewed',
        assigned_to = ?,
        review_decision = ?,
        decision_reason = ?,
        review_completed_at = GETDATE(),
        updated_at = GETDATE()
    WHERE 
        id = ? AND status = 'in_review'
    """
    
    try:
        # Begin transaction
        conn = get_connection()
        cursor = conn.cursor()
        
        # Update HITL queue
        cursor.execute(hitl_query, [user, decision, reason, queue_id])
        
        if cursor.rowcount == 0:
            conn.rollback()
            logger.error(f"Failed to update HITL queue record {queue_id}")
            return False
        
        # Get reconciliation ID from queue
        cursor.execute("SELECT reconciliation_id FROM hitl_queue WHERE id = ?", [queue_id])
        row = cursor.fetchone()
        
        if not row:
            conn.rollback()
            logger.error(f"Could not find reconciliation ID for queue record {queue_id}")
            return False
        
        reconciliation_id = row.reconciliation_id
        
        # Update reconciliation result based on decision
        if decision == 'approve':
            # If approving, update to 'matched'
            recon_query = """
            UPDATE reconciliation_result
            SET 
                match_status = 'matched',
                match_method = 'hitl',
                updated_at = GETDATE()
            WHERE 
                id = ?
            """
            cursor.execute(recon_query, [reconciliation_id])
        
        elif decision == 'reject':
            # If rejecting, update to 'unmatched'
            recon_query = """
            UPDATE reconciliation_result
            SET 
                match_status = 'unmatched',
                order_id = NULL,
                match_method = 'hitl',
                updated_at = GETDATE()
            WHERE 
                id = ?
            """
            cursor.execute(recon_query, [reconciliation_id])
        
        # For manual matches with a specific order ID
        if match_order_id:
            manual_match_query = """
            UPDATE reconciliation_result
            SET 
                match_status = 'matched',
                order_id = ?,
                match_method = 'hitl_manual',
                updated_at = GETDATE()
            WHERE 
                id = ?
            """
            cursor.execute(manual_match_query, [match_order_id, reconciliation_id])
        
        # Add to audit log
        audit_query = """
        INSERT INTO reconciliation_audit_log (
            entity_type,
            entity_id,
            action,
            field_name,
            old_value,
            new_value,
            reason,
            user_id
        )
        VALUES (
            'hitl_review',
            ?,
            ?,
            'status',
            'in_review',
            'reviewed',
            ?,
            ?
        )
        """
        cursor.execute(audit_query, [queue_id, decision, reason, user])
        
        # Commit transaction
        conn.commit()
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error submitting review: {e}")
        return False
    finally:
        conn.close()


def get_review_details(queue_id: int) -> Dict[str, Any]:
    """
    Get detailed information for a review.
    
    Args:
        queue_id: ID in hitl_queue table
        
    Returns:
        Dictionary with detailed review data
    """
    query = """
    SELECT 
        q.id AS queue_id,
        q.reconciliation_id,
        q.status,
        q.assigned_to,
        r.customer_name,
        r.po_number,
        r.match_status,
        r.confidence_score,
        r.match_method,
        r.order_id,
        r.shipment_id,
        r.match_details,
        o.style AS order_style,
        o.color AS order_color,
        o.size AS order_size,
        o.quantity AS order_quantity,
        o.delivery_method AS order_delivery_method,
        o.order_date,
        s.style_name AS shipment_style,
        s.color_name AS shipment_color,
        s.size_name AS shipment_size,
        s.quantity AS shipment_quantity,
        s.ship_date
    FROM 
        hitl_queue q
    JOIN 
        reconciliation_result r ON q.reconciliation_id = r.id
    LEFT JOIN 
        ORDERS_UNIFIED o ON r.order_id = o.id
    JOIN 
        FM_orders_shipped s ON r.shipment_id = s.id
    WHERE 
        q.id = ?
    """
    
    try:
        results = execute_query(query, [queue_id])
        if results:
            result = results[0]
            
            # Parse match details JSON if available
            if result.get('match_details'):
                try:
                    result['match_details'] = json.loads(result['match_details'])
                except:
                    # If JSON parsing fails, keep as string
                    pass
            
            # Get attribute scores
            attr_query = """
            SELECT 
                attribute_name,
                order_value,
                shipment_value,
                match_score,
                match_method,
                is_key_attribute,
                weight
            FROM 
                match_attribute_score
            WHERE 
                reconciliation_id = ?
            ORDER BY 
                is_key_attribute DESC, weight DESC
            """
            
            result['attribute_scores'] = execute_query(attr_query, [result['reconciliation_id']])
            
            return result
        else:
            return {}
    except Exception as e:
        logger.error(f"Error getting review details: {e}")
        return {}


def search_potential_matches(
    shipment_id: int,
    customer_name: str,
    po_number: str,
    style: Optional[str] = None,
    color: Optional[str] = None,
    size: Optional[str] = None,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Search for potential order matches for a shipment.
    
    Args:
        shipment_id: ID in FM_orders_shipped table
        customer_name: Customer name
        po_number: PO number
        style: Style code/name to search for
        color: Color code/name to search for
        size: Size to search for
        limit: Maximum number of records to return
        
    Returns:
        List of dictionaries with potential matches
    """
    query = """
    SELECT TOP (?)
        o.id AS order_id,
        o.customer_name,
        o.po_number,
        o.style,
        o.color,
        o.size,
        o.quantity,
        o.delivery_method,
        o.order_date,
        (CASE 
            WHEN o.style = ? THEN 3 
            WHEN o.style LIKE ? THEN 2
            ELSE 0
        END +
        CASE 
            WHEN o.color = ? THEN 2 
            WHEN o.color LIKE ? THEN 1
            ELSE 0
        END +
        CASE 
            WHEN o.size = ? THEN 1 
            ELSE 0
        END) AS match_score
    FROM 
        ORDERS_UNIFIED o
    LEFT JOIN 
        reconciliation_result r ON o.id = r.order_id
    WHERE 
        o.customer_name = ? AND
        o.po_number = ? AND
        (r.id IS NULL OR r.match_status = 'uncertain')
    ORDER BY 
        match_score DESC,
        o.order_date DESC
    """
    
    params = [
        limit,
        style or '',
        f"%{style or ''}%",
        color or '',
        f"%{color or ''}%",
        size or '',
        customer_name,
        po_number
    ]
    
    try:
        return execute_query(query, params)
    except Exception as e:
        logger.error(f"Error searching for potential matches: {e}")
        return []


def run_hitl_app():
    """
    Run the Streamlit HITL review application.
    """
    st.set_page_config(page_title="Order Matching HITL Review", layout="wide")
    
    # User authentication (simplified)
    if 'user' not in st.session_state:
        st.session_state.user = 'reviewer'  # In production, use proper authentication
    
    # Page header
    st.title("Order Matching HITL Review")
    
    # Sidebar with filter options
    with st.sidebar:
        st.header("Filters")
        customer_filter = st.selectbox(
            "Customer",
            ["All"] + [row['customer_name'] for row in execute_query_with_auth("SELECT DISTINCT customer_name FROM reconciliation_result ORDER BY customer_name")]
        )
        
        status_filter = st.multiselect(
            "Status",
            ["pending", "in_review"],
            default=["pending", "in_review"]
        )
        
        st.header("User")
        st.text(f"Logged in as: {st.session_state.user}")
        
        if st.button("Refresh Queue"):
            st.session_state.reviews_refreshed = True
    
    # Main content
    tab1, tab2, tab3 = st.tabs(["Review Queue", "Review Item", "Layer 3 - Quantity Resolution"])
    
    # Tab 1: Review Queue
    with tab1:
        st.header("Pending Reviews")
        
        # Get pending reviews
        if 'reviews_refreshed' not in st.session_state:
            st.session_state.reviews_refreshed = True
        
        if st.session_state.reviews_refreshed:
            pending_reviews = get_pending_reviews(assigned_to=st.session_state.user)
            st.session_state.pending_reviews = pending_reviews
            st.session_state.reviews_refreshed = False
        
        # Display reviews in a table
        if st.session_state.pending_reviews:
            # Create a DataFrame for display
            df = pd.DataFrame(st.session_state.pending_reviews)
            
            # Apply filters
            if customer_filter != "All":
                df = df[df['customer_name'] == customer_filter]
            
            if status_filter:
                df = df[df['status'].isin(status_filter)]
            
            # Format the DataFrame for display
            display_df = df[['queue_id', 'customer_name', 'po_number', 'confidence_score', 
                            'match_status', 'priority', 'status', 'assigned_to']].copy()
            
            display_df['confidence_score'] = display_df['confidence_score'].apply(lambda x: f"{x:.2f}" if x else "N/A")
            
            # Display table with action buttons
            for _, row in display_df.iterrows():
                col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 1, 1, 1, 1])
                with col1:
                    st.write(f"**{row['customer_name']}** - PO: {row['po_number']}")
                with col2:
                    st.write(f"Status: {row['match_status']} ({row['confidence_score']})")
                with col3:
                    st.write(f"Priority: {row['priority']}")
                with col4:
                    st.write(f"Queue: {row['status']}")
                with col5:
                    st.write(f"Assigned: {row['assigned_to'] or 'None'}")
                with col6:
                    if st.button("Review", key=f"review_{row['queue_id']}"):
                        # Claim the review
                        if row['status'] == 'pending':
                            if claim_review(row['queue_id'], st.session_state.user):
                                st.success(f"Review #{row['queue_id']} claimed")
                            else:
                                st.error("Failed to claim review")
                        
                        # Set the active review
                        st.session_state.active_review = row['queue_id']
                        st.session_state.active_tab = "Review Item"
                        st.experimental_rerun()
        else:
            st.info("No pending reviews in the queue")
    
    # Tab 2: Review Item
    with tab2:
        if 'active_review' in st.session_state:
            # Get details for the active review
            review = get_review_details(st.session_state.active_review)
            
            if review:
                # Header with basic info
                st.header(f"Review #{st.session_state.active_review}: {review['customer_name']} - PO {review['po_number']}")
                st.subheader(f"Match Status: {review['match_status']} (Confidence: {review['confidence_score']:.2f})")
                
                # Display comparison in columns
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Shipment Details")
                    st.write(f"Style: {review['shipment_style']}")
                    st.write(f"Color: {review['shipment_color']}")
                    st.write(f"Size: {review['shipment_size']}")
                    st.write(f"Quantity: {review['shipment_quantity']}")
                    st.write(f"Ship Date: {review['ship_date']}")
                
                with col2:
                    st.subheader("Order Details")
                    if review['order_id']:
                        st.write(f"Style: {review['order_style']}")
                        st.write(f"Color: {review['order_color']}")
                        st.write(f"Size: {review['order_size']}")
                        st.write(f"Quantity: {review['order_quantity']}")
                        st.write(f"Delivery Method: {review['order_delivery_method']}")
                        st.write(f"Order Date: {review['order_date']}")
                    else:
                        st.warning("No matching order found")
                
                # Display attribute comparison
                st.subheader("Attribute Comparison")
                if 'attribute_scores' in review:
                    attr_df = pd.DataFrame(review['attribute_scores'])
                    if not attr_df.empty:
                        # Format the scores
                        attr_df['match_score'] = attr_df['match_score'].apply(lambda x: f"{x:.2f}")
                        attr_df['is_key_attribute'] = attr_df['is_key_attribute'].apply(lambda x: "Yes" if x else "No")
                        
                        # Display as a styled table
                        st.dataframe(attr_df)
                    else:
                        st.info("No attribute comparison data available")
                
                # Search for alternative matches
                st.subheader("Search for Alternative Matches")
                search_col1, search_col2, search_col3 = st.columns(3)
                with search_col1:
                    search_style = st.text_input("Style", value=review['shipment_style'])
                with search_col2:
                    search_color = st.text_input("Color", value=review['shipment_color'])
                with search_col3:
                    search_size = st.text_input("Size", value=review['shipment_size'])
                
                if st.button("Search"):
                    potential_matches = search_potential_matches(
                        review['shipment_id'],
                        review['customer_name'],
                        review['po_number'],
                        search_style,
                        search_color,
                        search_size
                    )
                    
                    if potential_matches:
                        st.session_state.potential_matches = potential_matches
                    else:
                        st.warning("No potential matches found")
                        st.session_state.potential_matches = []
                
                # Display potential matches
                if 'potential_matches' in st.session_state and st.session_state.potential_matches:
                    st.subheader("Potential Matches")
                    match_df = pd.DataFrame(st.session_state.potential_matches)
                    
                    # Display each potential match with action buttons
                    for idx, row in match_df.iterrows():
                        match_col1, match_col2, match_col3 = st.columns([3, 2, 1])
                        with match_col1:
                            st.write(f"**Style:** {row['style']}, **Color:** {row['color']}, **Size:** {row['size']}")
                        with match_col2:
                            st.write(f"Quantity: {row['quantity']}, Score: {row['match_score']}")
                        with match_col3:
                            if st.button("Select", key=f"select_{idx}"):
                                st.session_state.selected_match = row['order_id']
                                st.session_state.selected_match_details = row
                
                # Review decision form
                st.subheader("Review Decision")
                
                decision = st.radio(
                    "Decision",
                    ["Approve Match", "Reject Match", "Manual Match", "Need More Information"]
                )
                
                reason = st.text_area("Reason for Decision")
                
                # Show selected match if doing a manual match
                if decision == "Manual Match":
                    if 'selected_match' in st.session_state:
                        st.success(f"Selected Order ID: {st.session_state.selected_match}")
                        if 'selected_match_details' in st.session_state:
                            details = st.session_state.selected_match_details
                            st.write(f"Style: {details['style']}, Color: {details['color']}, Size: {details['size']}")
                    else:
                        st.warning("Please select a match from the search results above")
                
                # Submit button
                if st.button("Submit Decision"):
                    if not reason:
                        st.error("Please provide a reason for your decision")
                    elif decision == "Manual Match" and 'selected_match' not in st.session_state:
                        st.error("Please select a match from the search results")
                    else:
                        # Map UI decision to backend values
                        decision_map = {
                            "Approve Match": "approve",
                            "Reject Match": "reject",
                            "Manual Match": "manual_match",
                            "Need More Information": "need_more_info"
                        }
                        
                        # Get order ID for manual match
                        match_order_id = None
                        if decision == "Manual Match":
                            match_order_id = st.session_state.selected_match
                        
                        # Submit the review
                        success = submit_review(
                            st.session_state.active_review,
                            st.session_state.user,
                            decision_map[decision],
                            reason,
                            match_order_id
                        )
                        
                        if success:
                            st.success("Review submitted successfully")
                            # Clear the active review and return to queue
                            if 'active_review' in st.session_state:
                                del st.session_state.active_review
                            if 'potential_matches' in st.session_state:
                                del st.session_state.potential_matches
                            if 'selected_match' in st.session_state:
                                del st.session_state.selected_match
                            if 'selected_match_details' in st.session_state:
                                del st.session_state.selected_match_details
                            
                            st.session_state.reviews_refreshed = True
                            st.experimental_rerun()
                        else:
                            st.error("Failed to submit review")
            else:
                st.error(f"Could not find review #{st.session_state.active_review}")
        else:
            st.info("Select a review from the queue to get started")
    
    # Tab 3: Layer 3 Quantity Resolution
    with tab3:
        st.header("🔧 Layer 3 - Quantity Resolution Analysis")
        st.info("Additional orders linked to resolve major quantity discrepancies")
        
        # Customer and PO selection
        col1, col2 = st.columns(2)
        with col1:
            customer_name = st.selectbox("Customer:", ["GREYSON"], index=0, key="layer3_customer")
        with col2:
            po_number = st.selectbox("PO Number:", ["4755"], index=0, key="layer3_po")
        
        if customer_name and po_number:
            show_layer3_analysis(customer_name, po_number)


def show_layer3_analysis(customer_name: str, po_number: str):
    """Show Layer 3 quantity resolution matches and analysis"""
    
    # Layer 3 matches query
    layer3_query = """
    SELECT 
        emr.shipment_id,
        emr.order_id,
        emr.shipment_style_code,
        emr.shipment_color_description,
        emr.shipment_quantity,
        emr.order_quantity,
        emr.quantity_difference_percent,
        emr.quantity_check_result,
        emr.shipment_delivery_method,
        emr.order_delivery_method,
        emr.delivery_match,
        emr.match_confidence,
        emr.created_at
    FROM enhanced_matching_results emr
    WHERE emr.customer_name = ?
        AND emr.po_number = ?
        AND emr.match_layer = 'LAYER_3'
    ORDER BY emr.shipment_id, emr.created_at
    """
    
    # Quantity analysis - before and after Layer 3
    quantity_analysis_query = """
    WITH shipment_totals AS (
        SELECT 
            shipment_id,
            shipment_style_code,
            shipment_color_description,
            shipment_quantity,
            shipment_delivery_method,
            COUNT(*) as order_count,
            SUM(order_quantity) as total_order_quantity,
            MIN(quantity_difference_percent) as best_variance_pct,
            MAX(CASE WHEN quantity_check_result = 'PASS' THEN 1 ELSE 0 END) as has_pass
        FROM enhanced_matching_results 
        WHERE customer_name = ? AND po_number = ?
        GROUP BY shipment_id, shipment_style_code, shipment_color_description, 
                 shipment_quantity, shipment_delivery_method
    )
    SELECT 
        shipment_id,
        shipment_style_code,
        shipment_color_description,
        shipment_quantity,
        total_order_quantity,
        (shipment_quantity - total_order_quantity) as quantity_gap,
        ABS((shipment_quantity - total_order_quantity) * 100.0 / shipment_quantity) as variance_pct,
        CASE WHEN has_pass = 1 THEN 'RESOLVED' ELSE 'FAILED' END as resolution_status,
        order_count,
        CASE 
            WHEN order_count > 1 THEN 'LAYER_3_APPLIED' 
            ELSE 'SINGLE_MATCH' 
        END as matching_type
    FROM shipment_totals
    ORDER BY variance_pct DESC
    """
    
    try:
        # Get Layer 3 matches
        layer3_matches = execute_query_with_auth(layer3_query, [customer_name, po_number])
        
        # Get quantity analysis  
        quantity_analysis = execute_query_with_auth(quantity_analysis_query, [customer_name, po_number])
        
        if layer3_matches:
            st.success(f"🎉 {len(layer3_matches)} Layer 3 matches applied!")
            
            # Show Layer 3 matches with status
            st.markdown("### 📋 Layer 3 Additional Order Links")
            
            layer3_df = pd.DataFrame(layer3_matches)
            layer3_df['Status'] = layer3_df['quantity_check_result'].apply(
                lambda x: '✅ RESOLVED' if x == 'PASS' else '❌ FAILED'
            )
            layer3_df['Delivery'] = layer3_df['delivery_match'].apply(
                lambda x: '🟢 MATCH' if x == 'MATCH' else '🟡 MISMATCH'  
            )
            layer3_df['Variance'] = layer3_df['quantity_difference_percent'].round(1).astype(str) + '%'
            
            display_cols = [
                'shipment_id', 'shipment_style_code', 'shipment_color_description',
                'shipment_quantity', 'order_quantity', 'Variance', 'Status', 'Delivery'
            ]
            
            st.dataframe(
                layer3_df[display_cols],
                use_container_width=True,
                hide_index=True
            )
        
        # Show quantity resolution analysis
        st.markdown("### 📊 Quantity Resolution Analysis")
        
        if quantity_analysis:
            analysis_df = pd.DataFrame(quantity_analysis)
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            total_shipments = len(analysis_df)
            resolved_shipments = len(analysis_df[analysis_df['resolution_status'] == 'RESOLVED'])
            layer3_shipments = len(analysis_df[analysis_df['matching_type'] == 'LAYER_3_APPLIED'])
            avg_variance = analysis_df['variance_pct'].mean()
            
            with col1:
                st.metric("Total Shipments", total_shipments)
            with col2:
                st.metric("Resolved", resolved_shipments, delta=f"{resolved_shipments/total_shipments*100:.1f}%")
            with col3:
                st.metric("Layer 3 Applied", layer3_shipments)
            with col4:
                st.metric("Avg Variance", f"{avg_variance:.1f}%")
            
            # Detailed analysis table
            st.markdown("### 🔍 Shipment-Level Resolution Status")
            
            analysis_df['Resolution'] = analysis_df['resolution_status'].apply(
                lambda x: '✅ RESOLVED' if x == 'RESOLVED' else '❌ FAILED'
            )
            analysis_df['Matching'] = analysis_df['matching_type'].apply(
                lambda x: '🔧 Layer 3' if x == 'LAYER_3_APPLIED' else '🎯 Single'
            )
            analysis_df['Variance %'] = analysis_df['variance_pct'].round(1)
            
            # Display with color coding
            def highlight_resolution(row):
                if row['resolution_status'] == 'RESOLVED':
                    return ['background-color: #d4edda'] * len(row)
                else:
                    return ['background-color: #f8d7da'] * len(row)
            
            styled_analysis = analysis_df[[
                'shipment_id', 'shipment_style_code', 'shipment_color_description',
                'shipment_quantity', 'total_order_quantity', 'quantity_gap', 
                'Variance %', 'Resolution', 'Matching', 'order_count'
            ]].style.apply(highlight_resolution, axis=1)
            
            st.dataframe(styled_analysis, use_container_width=True, hide_index=True)
            
            # Success story
            if layer3_shipments > 0:
                st.success(f"""
                🎉 **Layer 3 Success Story**:
                - {layer3_shipments} shipments had additional orders linked
                - {resolved_shipments} total shipments now have acceptable variance (≤10%)
                - Major quantity gaps resolved through intelligent order consolidation
                """)
            
            # Remaining issues
            failed_shipments = analysis_df[analysis_df['resolution_status'] == 'FAILED']
            if not failed_shipments.empty:
                st.warning(f"""
                ⚠️ **Remaining Quantity Issues**:
                - {len(failed_shipments)} shipments still have >10% variance
                - These may need manual investigation or have acceptable business tolerance
                - Consider Layer 4 matching with CANCELLED orders for historical context
                """)
        
        else:
            st.info("No quantity analysis data available")
            
    except Exception as e:
        st.error(f"Error loading Layer 3 analysis: {e}")
        logger.error(f"Layer 3 analysis error: {e}")


if __name__ == "__main__":
    run_hitl_app()
