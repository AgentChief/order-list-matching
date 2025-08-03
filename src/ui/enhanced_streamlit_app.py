"""
Enhanced Streamlit app with Layer 1 and Layer 2 matching support
"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path
import os

# --- Fix for Streamlit import path issues ---
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- End fix ---

# --- Robust import for auth_helper ---
import importlib.util
import sys
from pathlib import Path
project_root = str(Path(__file__).resolve().parents[2])
auth_helper_path = str(Path(project_root) / 'auth_helper.py')
if auth_helper_path not in sys.modules:
    spec = importlib.util.spec_from_file_location('auth_helper', auth_helper_path)
    auth_helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(auth_helper)
    sys.modules['auth_helper'] = auth_helper
get_connection_string = auth_helper.get_connection_string
# --- End robust import ---

from src.ui.enhanced_matching_tabs import (
    show_layer0_exact_matches,
    show_layer1_layer2_matches,
    show_layer3_quantity_resolution,
    show_enhanced_delivery_review,
    show_enhanced_quantity_review,
    show_unmatched_shipments
)

class EnhancedConfigManager:
    def __init__(self):
        self.connection_string = get_connection_string()
        
    def get_connection(self):
        """Get database connection"""
        import pyodbc
        return pyodbc.connect(self.connection_string)
    
    def execute_query(self, query, params=None):
        """Execute query and return results as DataFrame"""
        import pyodbc
        with self.get_connection() as conn:
            return pd.read_sql(query, conn, params=params)

def show_enhanced_hitl_review():
    """Enhanced Human-in-the-Loop review with layered matching"""
    st.title("🎯 Enhanced Order-Shipment Matching Review")
    st.markdown("**New Layer-based Matching System with Bulk Approval**")
    
    config_mgr = EnhancedConfigManager()
    
    # Customer and PO selection
    col1, col2 = st.columns(2)
    
    with col1:
        customer_filter = st.selectbox(
            "Select Customer:",
            ["GREYSON", "JOHNNIE_O", "ALL"],
            help="Choose customer for review"
        )
    
    with col2:
        po_filter = st.text_input(
            "PO Number:", 
            value="4755",
            help="Enter PO number to review"
        )
    
    if not customer_filter or not po_filter:
        st.warning("Please select customer and enter PO number")
        return
    
    # Get matching summary (from cache)
    try:
        summary_query = """
        SELECT 
            shipment_status,
            COUNT(*) as match_count,
            AVG(best_confidence) as avg_confidence,
            SUM(CASE WHEN quantity_variance = 0 THEN 1 ELSE 0 END) as qty_pass,
            SUM(CASE WHEN ABS(quantity_variance) > 0 THEN 1 ELSE 0 END) as qty_fail
        FROM shipment_summary_cache
        WHERE customer_name LIKE ? AND (? IS NULL OR shipment_id IN (SELECT shipment_id FROM shipment_summary_cache WHERE customer_name LIKE ?))
        GROUP BY shipment_status
        ORDER BY shipment_status
        """
        summary_results = config_mgr.execute_query(
            summary_query, 
            [f"{customer_filter}%", None, f"{customer_filter}%"]
        )
        
        # Display summary metrics
        st.markdown("### 📊 Matching Summary")
        
        if not summary_results.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            total_matches = summary_results['match_count'].sum()
            layer0_count = summary_results[summary_results['shipment_status'] == 'LAYER_0']['match_count'].sum() if 'LAYER_0' in summary_results['shipment_status'].values else 0
            layer1_count = summary_results[summary_results['shipment_status'] == 'LAYER_1']['match_count'].sum() if 'LAYER_1' in summary_results['shipment_status'].values else 0
            layer2_count = summary_results[summary_results['shipment_status'] == 'LAYER_2']['match_count'].sum() if 'LAYER_2' in summary_results['shipment_status'].values else 0
            
            with col1:
                st.metric("🎯 Layer 0 (Exact)", layer0_count)
            with col2:
                st.metric("🔄 Layer 1 (Fuzzy)", layer1_count)
            with col3:
                st.metric("🔍 Layer 2 (Deep)", layer2_count)
            with col4:
                st.metric("📊 Total Matches", total_matches)
            
            # Match quality metrics
            col1, col2, col3 = st.columns(3)
            
            total_qty_pass = summary_results['qty_pass'].sum()
            total_qty_fail = summary_results['qty_fail'].sum()
            
            with col1:
                avg_confidence = summary_results['avg_confidence'].mean()
                st.metric("🎯 Avg Confidence", f"{avg_confidence:.1%}" if pd.notna(avg_confidence) else "N/A")
            with col2:
                st.metric("✅ Qty Pass", total_qty_pass)
            with col3:
                st.metric("❌ Qty Issues", total_qty_fail)
        
        # Enhanced tabbed interface
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📋 All Shipments", 
            "🎯 Layer 0-Exact", 
            "🔄 Layer 1&2-Fuzzy", 
            "🚚 Delivery Review", 
            "⚖️ Quantity Review",
            "❌ Unmatched"
        ])
        
        with tab1:
            st.subheader("📦 All Shipments Overview")
            show_all_shipments_enhanced(customer_filter, po_filter, config_mgr)
        
        with tab2:
            show_layer0_exact_matches(customer_filter, config_mgr)
        
        with tab3:
            show_layer1_layer2_matches(customer_filter, config_mgr)
        
        with tab4:
            show_enhanced_delivery_review(customer_filter, config_mgr)
        
        with tab5:
            show_enhanced_quantity_review(customer_filter, config_mgr)
        
        with tab6:
            show_unmatched_shipments(customer_filter, config_mgr)
            
    except Exception as e:
        st.error(f"Error loading enhanced review: {str(e)}")
        st.exception(e)

def show_all_shipments_enhanced(customer_filter, po_filter, config_mgr):
    """Enhanced all shipments view with cache"""
    query = """
    SELECT 
        shipment_id,
        style_code,
        color_description,
        delivery_method,
        quantity as shipment_qty,
        last_updated,
        shipment_status,
        best_confidence as confidence,
        quantity_variance as qty_diff,
        CASE 
            WHEN shipment_status = 'GOOD' THEN '✅ Good'
            WHEN shipment_status = 'QUANTITY_ISSUES' THEN '❌ Qty Issue'
            WHEN shipment_status = 'DELIVERY_ISSUES' THEN '❌ Delivery Issue'
            WHEN shipment_status = 'UNMATCHED' THEN '❌ Unmatched'
            ELSE '❓ Unknown'
        END as status_display
    FROM shipment_summary_cache
    WHERE customer_name LIKE ? AND (? IS NULL OR shipment_id IN (SELECT shipment_id FROM shipment_summary_cache WHERE customer_name LIKE ?))
    ORDER BY shipment_status, shipment_id
    """
    
    try:
        all_shipments = config_mgr.execute_query(query, [f"{customer_filter}%", None, f"{customer_filter}%"])
        
        if not all_shipments.empty:
            st.success(f"Found {len(all_shipments)} shipments")
            
            # Add color coding
            def style_row(row):
                if row['shipment_status'] == 'LAYER_0':
                    return ['background-color: #90EE90'] * len(row)  # Light green
                elif row['shipment_status'] == 'LAYER_1':
                    return ['background-color: #FFE4B5'] * len(row)  # Light orange
                elif row['shipment_status'] == 'LAYER_2':
                    return ['background-color: #87CEEB'] * len(row)  # Light blue
                elif row['shipment_status'] == 'UNMATCHED':
                    return ['background-color: #FFB6C1'] * len(row)  # Light red
                return [''] * len(row)
            
            # Display enhanced table
            display_cols = [
                'shipment_id', 'style_code', 'color_description', 'delivery_method',
                'shipment_qty', 'status_display', 'confidence', 'qty_diff'
            ]
            
            styled_df = all_shipments[display_cols].style.apply(style_row, axis=1)
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            # Summary by layer
            layer_summary = all_shipments['shipment_status'].value_counts()
            st.markdown("### 📊 Match Distribution")
            
            for status, count in layer_summary.items():
                if status == 'LAYER_0':
                    st.success(f"🎯 Layer 0 (Perfect): {count}")
                elif status == 'LAYER_1':
                    st.info(f"🔄 Layer 1 (Fuzzy): {count}")
                elif status == 'LAYER_2':
                    st.info(f"🔍 Layer 2 (Deep): {count}")
                elif status == 'UNMATCHED':
                    st.error(f"❌ Unmatched: {count}")
        
        else:
            st.warning("No shipments found for the selected criteria")
            
    except Exception as e:
        st.error(f"Error loading shipments: {e}")

def show_layer3_review():
    """Show Layer 3 quantity resolution review page"""
    st.title("🔧 Layer 3 - Quantity Resolution Analysis")
    st.markdown("**Review quantity variance improvements through additional order linking**")
    
    config_mgr = EnhancedConfigManager()
    
    # Customer and PO selection
    col1, col2 = st.columns(2)
    with col1:
        customer_name = st.selectbox("Customer:", ["GREYSON"], index=0)
    with col2:
        po_number = st.selectbox("PO Number:", ["4755"], index=0)
    
    if customer_name and po_number:
        show_layer3_quantity_resolution(customer_name, config_mgr)

def main():
    """Main Streamlit app"""
    st.set_page_config(
        page_title="Enhanced Order Matching", 
        page_icon="🎯",
        layout="wide"
    )
    
    st.sidebar.title("🎯 Enhanced Matching System")
    st.sidebar.markdown("**Layer-based matching with bulk approval**")
    
    page = st.sidebar.selectbox(
        "Navigation:",
        [
            "Enhanced HITL Review",
            "Layer 3 - Quantity Resolution",
            "Configuration Management", 
            "System Status"
        ]
    )
    
    if page == "Enhanced HITL Review":
        show_enhanced_hitl_review()
    elif page == "Layer 3 - Quantity Resolution":
        show_layer3_review()
    elif page == "Configuration Management":
        st.title("⚙️ Configuration Management")
        st.info("Configuration management features coming soon...")
    else:
        st.title("📊 System Status")
        st.info("System status monitoring coming soon...")

if __name__ == "__main__":
    main()
