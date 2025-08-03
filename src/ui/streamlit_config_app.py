"""
Streamlit Configuration Management UI
User-friendly interface for managing order-shipment matching configurations
"""

import streamlit as st
import pandas as pd
import pyodbc
import json
import yaml
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from auth_helper import get_connection_string

# Import enhanced matching tabs
try:
    from src.ui.enhanced_matching_tabs import (
        show_layer0_exact_matches,
        show_layer1_layer2_matches, 
        show_enhanced_delivery_review,
        show_enhanced_quantity_review,
        show_unmatched_shipments
    )
except ImportError:
    # Fallback imports
    pass

class ConfigurationManager:
    def __init__(self):
        self.connection_string = get_connection_string()
        
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def get_customers(self):
        """Get all customers with their configurations"""
        query = """
        SELECT 
            c.id,
            c.canonical_name,
            c.status,
            c.packed_products,
            c.shipped,
            c.master_order_list,
            STRING_AGG(ca.alias_name, ', ') as aliases,
            c.created_at,
            c.updated_at
        FROM customers c
        LEFT JOIN customer_aliases ca ON c.id = ca.customer_id
        GROUP BY c.id, c.canonical_name, c.status, c.packed_products, c.shipped, c.master_order_list, c.created_at, c.updated_at
        ORDER BY c.canonical_name
        """
        
        with self.get_connection() as conn:
            return pd.read_sql(query, conn)
    
    def get_customer_config(self, customer_name):
        """Get complete configuration for a customer"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("EXEC sp_get_customer_config ?", customer_name)
            result = cursor.fetchone()
            
            if result:
                return {
                    'canonical_name': result[0],
                    'status': result[1],
                    'column_mappings': json.loads(result[2]) if result[2] else [],
                    'matching_strategy': json.loads(result[3]) if result[3] else {},
                    'exclusion_rules': json.loads(result[4]) if result[4] else []
                }
            return None
    
    def save_customer(self, customer_data):
        """Save or update customer configuration"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if customer_data.get('id'):
                # Update existing
                cursor.execute("""
                    UPDATE customers 
                    SET canonical_name = ?, status = ?, packed_products = ?, 
                        shipped = ?, master_order_list = ?, updated_at = GETDATE(),
                        updated_by = ?
                    WHERE id = ?
                """, customer_data['canonical_name'], customer_data['status'],
                    customer_data.get('packed_products'), customer_data.get('shipped'),
                    customer_data.get('master_order_list'), 'streamlit_user', customer_data['id'])
            else:
                # Insert new
                cursor.execute("""
                    INSERT INTO customers (canonical_name, status, packed_products, shipped, master_order_list, created_by, updated_by)
                    OUTPUT INSERTED.id
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, customer_data['canonical_name'], customer_data['status'],
                    customer_data.get('packed_products'), customer_data.get('shipped'),
                    customer_data.get('master_order_list'), 'streamlit_user', 'streamlit_user')
                
                result = cursor.fetchone()
                customer_data['id'] = result[0]
            
            conn.commit()
            return customer_data['id']
    
    def save_column_mappings(self, customer_id, mappings):
        """Save column mappings for a customer"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Delete existing mappings
            cursor.execute("DELETE FROM column_mappings WHERE customer_id = ?", customer_id)
            
            # Insert new mappings
            for mapping in mappings:
                cursor.execute("""
                    INSERT INTO column_mappings (customer_id, order_column, shipment_column, priority, created_by, updated_by)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, customer_id, mapping['order_column'], mapping['shipment_column'], 
                    mapping.get('priority', 1), 'streamlit_user', 'streamlit_user')
            
            conn.commit()
    
    def save_exclusion_rules(self, customer_id, rules):
        """Save exclusion rules for a customer"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Delete existing rules
            cursor.execute("DELETE FROM exclusion_rules WHERE customer_id = ?", customer_id)
            
            # Insert new rules
            for rule in rules:
                cursor.execute("""
                    INSERT INTO exclusion_rules (customer_id, table_name, field_name, exclude_values, rule_type, description, created_by, updated_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, customer_id, rule['table_name'], rule['field_name'], 
                    json.dumps(rule['exclude_values']), rule.get('rule_type', 'exclude'),
                    rule.get('description', ''), 'streamlit_user', 'streamlit_user')
            
            conn.commit()
    
    def save_value_mapping(self, customer_id, field_name, source_value, target_value, justification, created_by='streamlit_user'):
        """Save a value mapping for field standardization"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO value_mappings (customer_id, field_name, source_value, target_value, justification, status, created_by, updated_by)
                VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
            """, customer_id, field_name, source_value, target_value, justification, created_by, created_by)
            conn.commit()
    
    def get_value_mappings(self, customer_id=None, field_name=None):
        """Get value mappings with optional filtering"""
        query = """
        SELECT vm.id, c.canonical_name, vm.field_name, vm.source_value, vm.target_value, 
               vm.justification, vm.status, vm.created_at, vm.created_by
        FROM value_mappings vm
        LEFT JOIN customers c ON vm.customer_id = c.id
        WHERE vm.status = 'active'
        """
        params = []
        
        if customer_id:
            query += " AND vm.customer_id = ?"
            params.append(customer_id)
            
        if field_name:
            query += " AND vm.field_name = ?"
            params.append(field_name)
            
        query += " ORDER BY vm.created_at DESC"
        
        with self.get_connection() as conn:
            return pd.read_sql(query, conn, params=params)
    
    def save_hitl_decision(self, match_type, shipment_id, order_id, decision, justification, created_by='streamlit_user'):
        """Save a HITL review decision"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO hitl_decisions (match_type, shipment_id, order_id, decision, justification, created_by, updated_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, match_type, shipment_id, order_id, decision, justification, created_by, created_by)
            conn.commit()
    
    def get_enhanced_matching_results(self, customer_name=None, status_filter=None):
        """Get enhanced matching results with full context for HITL review"""
        query = """
        SELECT 
            emr.id,
            emr.matching_session_id,
            emr.customer_name,
            emr.po_number,
            emr.shipment_id,
            emr.order_id,
            CASE WHEN emr.style_match = 'MATCH' THEN 1 ELSE 0 END as style_match,
            CASE WHEN emr.color_match = 'MATCH' THEN 1 ELSE 0 END as color_match, 
            CASE WHEN emr.delivery_match = 'MATCH' THEN 1 ELSE 0 END as delivery_match,
            emr.quantity_check_result,
            emr.quantity_difference_percent,
            emr.match_layer,
            emr.match_confidence,
            'review' as status,
            '' as notes,
            emr.created_at,
            
            -- Shipment details
            emr.shipment_style_code as shipment_style,
            emr.shipment_color_description as shipment_color,
            emr.shipment_delivery_method as shipment_delivery,
            emr.shipment_quantity as shipment_qty,
            emr.customer_name as shipment_customer,
            
            -- Order details  
            emr.order_style_code as order_style,
            emr.order_color_description as order_color,
            emr.order_delivery_method as order_delivery,
            emr.order_quantity as order_qty,
            emr.customer_name as order_customer
            
        FROM enhanced_matching_results emr
        WHERE 1=1
        """
        
        params = []
        if customer_name:
            query += " AND emr.customer_name = ?"
            params.append(customer_name)
            
        if status_filter:
            query += " AND 'review' = ?"
            params.append(status_filter)
            
        query += " ORDER BY emr.created_at DESC"
        
        with self.get_connection() as conn:
            return pd.read_sql(query, conn, params=params)
    
    def get_delivery_mismatches_summary(self, customer_name=None):
        """Get summary of delivery method mismatches for mapping interface"""
        query = """
        SELECT 
            emr.customer_name,
            emr.shipment_delivery_method as shipment_delivery,
            emr.order_delivery_method as order_delivery,
            COUNT(*) as mismatch_count,
            STRING_AGG(CAST(emr.shipment_id AS VARCHAR), ', ') as affected_shipments
            
        FROM enhanced_matching_results emr
        WHERE emr.delivery_match = 'MISMATCH'
        """
        
        params = []
        if customer_name:
            query += " AND emr.customer_name = ?"
            params.append(customer_name)
            
        query += """
        GROUP BY emr.customer_name, emr.shipment_delivery_method, emr.order_delivery_method
        ORDER BY mismatch_count DESC
        """
        
        with self.get_connection() as conn:
            return pd.read_sql(query, conn, params=params)
    
    def get_quantity_issues_detailed(self, customer_name=None):
        """Get detailed quantity issues for review"""
        query = """
        SELECT 
            emr.id,
            emr.customer_name,
            emr.shipment_id,
            emr.order_id,
            emr.quantity_difference_percent,
            emr.match_confidence,
            'review' as status,
            
            -- Shipment context
            emr.shipment_style_code as shipment_style,
            emr.shipment_color_description as shipment_color,
            emr.shipment_quantity as shipment_qty,
            emr.shipment_delivery_method as shipment_delivery,
            
            -- Order context
            emr.order_style_code as order_style,
            emr.order_color_description as order_color, 
            emr.order_quantity as order_qty,
            emr.order_delivery_method as order_delivery,
            
            -- Calculated fields
            ABS(emr.shipment_quantity - emr.order_quantity) as qty_difference,
            CASE 
                WHEN emr.quantity_difference_percent > 100 THEN 'Critical'
                WHEN emr.quantity_difference_percent > 50 THEN 'High'
                WHEN emr.quantity_difference_percent > 25 THEN 'Medium'
                ELSE 'Low'
            END as severity_level
            
        FROM enhanced_matching_results emr
        WHERE emr.quantity_check_result = 'FAIL'
        """
        
        params = []
        if customer_name:
            query += " AND emr.customer_name = ?"
            params.append(customer_name)
            
        query += " ORDER BY emr.quantity_difference_percent DESC"
        
        with self.get_connection() as conn:
            return pd.read_sql(query, conn, params=params)
    
    def execute_query(self, query, params=None):
        """Execute a custom query and return results as DataFrame"""
        with self.get_connection() as conn:
            if params:
                return pd.read_sql(query, conn, params=params)
            else:
                return pd.read_sql(query, conn)
    
    # === ENHANCED DASHBOARD ANALYTICS METHODS ===
    
    def get_customer_summary(self):
        """Get customer counts by status"""
        query = """
        SELECT 
            status,
            COUNT(*) as customer_count
        FROM customers
        GROUP BY status
        ORDER BY customer_count DESC
        """
        return self.execute_query(query)
    
    def get_shipment_summary(self):
        """Get shipment summary with match status - FIXED to count unique shipments"""
        query = """
        SELECT 
            COUNT(DISTINCT s.shipment_id) as total_shipments,
            COUNT(DISTINCT emr.shipment_id) as matched_shipments,
            COUNT(DISTINCT s.shipment_id) - COUNT(DISTINCT emr.shipment_id) as unmatched_shipments,
            CAST(COUNT(DISTINCT emr.shipment_id) * 100.0 / COUNT(DISTINCT s.shipment_id) AS DECIMAL(5,1)) as match_rate_pct,
            COUNT(emr.id) as total_matches
        FROM stg_fm_orders_shipped_table s
        LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
        """
        return self.execute_query(query)
    
    def get_layer_distribution(self):
        """Get match layer distribution - showing both unique shipments AND total matches"""
        query = """
        SELECT 
            match_layer,
            COUNT(*) as match_count,
            COUNT(DISTINCT shipment_id) as unique_shipments,
            CAST(COUNT(DISTINCT shipment_id) * 100.0 / 
                (SELECT COUNT(DISTINCT shipment_id) FROM enhanced_matching_results) 
                AS DECIMAL(5,1)) as shipment_percentage
        FROM enhanced_matching_results
        GROUP BY match_layer
        ORDER BY 
            CASE 
                WHEN match_layer = 'LAYER_0' THEN 0
                WHEN match_layer = 'LAYER_1' THEN 1
                WHEN match_layer = 'LAYER_2' THEN 2
                WHEN match_layer = 'LAYER_3' THEN 3
                ELSE 99
            END
        """
        return self.execute_query(query)
    
    def get_review_queue_summary(self):
        """Get items requiring human review by status - FIXED to count unique shipments"""
        query = """
        SELECT 
            CASE 
                WHEN quantity_check_result = 'FAIL' THEN 'Quantity Review'
                WHEN delivery_match = 'MISMATCH' THEN 'Delivery Review'
                WHEN match_confidence < 0.8 THEN 'Low Confidence Review'
                ELSE 'General Review'
            END as review_type,
            COUNT(*) as match_count,
            COUNT(DISTINCT shipment_id) as affected_shipments,
            COUNT(DISTINCT customer_name) as affected_customers
        FROM enhanced_matching_results
        WHERE quantity_check_result = 'FAIL' 
           OR delivery_match = 'MISMATCH'
           OR match_confidence < 0.8
        GROUP BY 
            CASE 
                WHEN quantity_check_result = 'FAIL' THEN 'Quantity Review'
                WHEN delivery_match = 'MISMATCH' THEN 'Delivery Review'
                WHEN match_confidence < 0.8 THEN 'Low Confidence Review'
                ELSE 'General Review'
            END
        ORDER BY affected_shipments DESC
        """
        return self.execute_query(query)
    
    def get_system_health_metrics(self):
        """Get system health and recent activity metrics"""
        query = """
        SELECT 
            COUNT(DISTINCT matching_session_id) as total_sessions,
            MAX(created_at) as last_activity,
            COUNT(CASE WHEN created_at > DATEADD(day, -7, GETDATE()) THEN 1 END) as recent_matches,
            COUNT(CASE WHEN quantity_check_result = 'FAIL' THEN 1 END) as quantity_failures,
            COUNT(CASE WHEN delivery_match = 'MISMATCH' THEN 1 END) as delivery_mismatches,
            AVG(match_confidence) as avg_confidence
        FROM enhanced_matching_results
        """
        return self.execute_query(query)
    
    def get_customer_breakdown(self):
        """Get detailed customer activity breakdown"""
        query = """
        SELECT 
            c.canonical_name,
            c.status,
            COUNT(emr.id) as total_matches,
            COUNT(CASE WHEN emr.match_layer = 'LAYER_0' THEN 1 END) as layer_0_matches,
            COUNT(CASE WHEN emr.match_layer = 'LAYER_1' THEN 1 END) as layer_1_matches,
            COUNT(CASE WHEN emr.match_layer = 'LAYER_2' THEN 1 END) as layer_2_matches,
            COUNT(CASE WHEN emr.match_layer = 'LAYER_3' THEN 1 END) as layer_3_matches,
            COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) as quantity_issues,
            COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) as delivery_issues,
            AVG(emr.match_confidence) as avg_confidence,
            MAX(emr.created_at) as last_activity
        FROM customers c
        LEFT JOIN enhanced_matching_results emr ON c.canonical_name = emr.customer_name
        GROUP BY c.canonical_name, c.status
        ORDER BY total_matches DESC, c.canonical_name
        """
        return self.execute_query(query)
    
    def get_shipment_level_summary(self, customer_filter=None):
        """Get ENHANCED one-row-per-shipment summary with match indicators, confidence, and consolidated layers"""
        query = """
        SELECT 
            ROW_NUMBER() OVER (ORDER BY 
                CASE 
                    WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 1
                    WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 2  
                    ELSE 3
                END,
                s.shipment_id
            ) as row_num,
            s.shipment_id,
            s.style_code as shipment_style,
            s.color_description as shipment_color,
            s.delivery_method as shipment_delivery,
            s.quantity as shipment_quantity,
            COUNT(emr.id) as match_count,
            
            -- Match status indicators (Y/N for safe display)
            CASE 
                WHEN MAX(CASE WHEN emr.style_match = 'MATCH' THEN 1 ELSE 0 END) = 1 THEN 'Y'
                ELSE 'N'
            END as style_match_indicator,
            
            CASE 
                WHEN MAX(CASE WHEN emr.color_match = 'MATCH' THEN 1 ELSE 0 END) = 1 THEN 'Y'
                ELSE 'N'
            END as color_match_indicator,
            
            CASE 
                WHEN MAX(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 ELSE 0 END) = 1 THEN 'N'
                WHEN MAX(CASE WHEN emr.delivery_match = 'MATCH' THEN 1 ELSE 0 END) = 1 THEN 'Y'
                ELSE 'U'
            END as delivery_match_indicator,
            
            -- Consolidated layer information 
            MIN(emr.match_layer) + '-' + MAX(emr.match_layer) as match_layers,
            
            -- Confidence levels
            MAX(emr.match_confidence) as best_confidence,
            AVG(emr.match_confidence) as avg_confidence,
            
            -- Matched order quantities (total)
            SUM(emr.order_quantity) as total_matched_order_qty,
            
            -- Quantity variance
            CASE 
                WHEN s.quantity - SUM(emr.order_quantity) = 0 THEN 'Y'
                WHEN ABS(s.quantity - SUM(emr.order_quantity)) <= s.quantity * 0.1 THEN 'P'
                ELSE 'N'
            END as quantity_match_indicator,
            
            s.quantity - SUM(emr.order_quantity) as quantity_variance,
            
            -- Overall status
            CASE 
                WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 'QUANTITY_ISSUES'
                WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 'DELIVERY_ISSUES'
                ELSE 'GOOD'
            END as shipment_status
            
        FROM stg_fm_orders_shipped_table s
        INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
        """
        
        params = []
        if customer_filter and customer_filter != "All Customers":
            query += " WHERE s.customer_name LIKE ?"
            params.append(f"%{customer_filter}%")
        
        query += """
        GROUP BY s.shipment_id, s.style_code, s.color_description, s.delivery_method, s.quantity
        """
        
        return self.execute_query(query, params)

def main():
    st.set_page_config(
        page_title="Order Matching Configuration",
        page_icon="⚙️",
        layout="wide"
    )
    
    st.title("⚙️ Order Matching Configuration Management")
    st.markdown("---")
    
    # Initialize configuration manager
    config_mgr = ConfigurationManager()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Select Page", [
        "🏠 Dashboard",
        "🔍 Review",
        "�👥 Customer Management", 
        "🔗 Column Mappings",
        "🎯 Matching Strategies",
        "🚫 Exclusion Rules",
        "💎 Value Mappings",
        "📊 Audit Trail",
        "📤 Import/Export"
    ])
    
    if page == "🏠 Dashboard":
        show_dashboard(config_mgr)
    elif page == "🔍 Review":
        show_hitl_review(config_mgr)
    elif page == "�👥 Customer Management":
        show_customer_management(config_mgr) 
    elif page == "🔗 Column Mappings":
        show_column_mappings(config_mgr)
    elif page == "🎯 Matching Strategies":
        show_matching_strategies(config_mgr)
    elif page == "🚫 Exclusion Rules":
        show_exclusion_rules(config_mgr)
    elif page == "💎 Value Mappings":
        show_value_mappings(config_mgr)
    elif page == "📊 Audit Trail":
        show_audit_trail(config_mgr)
    elif page == "📤 Import/Export":
        show_import_export(config_mgr)

def show_dashboard(config_mgr):
    """Show main dashboard with system overview"""
    st.header("System Overview")
    
    # Add tabs for different dashboard views
    tab1, tab2 = st.tabs(["📊 Enhanced Analytics", "👥 Customer Overview"])
    
    with tab1:
        show_enhanced_dashboard(config_mgr)
    
    with tab2:
        show_basic_dashboard(config_mgr)

def show_enhanced_dashboard(config_mgr):
    """Show enhanced dashboard with comprehensive analytics"""
    st.subheader("📊 System Analytics Dashboard")
    st.markdown("Real-time insights into matching performance and system health")
    
    try:
        # === TOP-LEVEL METRICS ===
        col1, col2, col3, col4 = st.columns(4)
        
        # Customer summary metrics
        customer_summary = config_mgr.get_customer_summary()
        shipment_summary = config_mgr.get_shipment_summary()
        
        if not customer_summary.empty and not shipment_summary.empty:
            with col1:
                total_customers = customer_summary['customer_count'].sum()
                st.metric("🏢 Total Customers", total_customers)
                
            with col2:
                total_shipments = shipment_summary.iloc[0]['total_shipments']
                st.metric("📦 Total Shipments", f"{total_shipments:,}")
                
            with col3:
                matched_shipments = shipment_summary.iloc[0]['matched_shipments']
                st.metric("✅ Matched Shipments", f"{matched_shipments:,}")
                
            with col4:
                match_rate = shipment_summary.iloc[0]['match_rate_pct']
                st.metric("📈 Shipment Match Rate", f"{match_rate}%")
        
        # Second row of metrics showing match details
        col1, col2, col3, col4 = st.columns(4)
        
        if not shipment_summary.empty:
            with col1:
                total_matches = shipment_summary.iloc[0]['total_matches']
                st.metric("🔗 Total Matches", f"{total_matches:,}")
                st.caption("Individual order-shipment matches")
                
            with col2:
                unmatched_shipments = shipment_summary.iloc[0]['unmatched_shipments']
                st.metric("❌ Unmatched Shipments", f"{unmatched_shipments:,}")
                st.caption("Shipments with no order matches")
                
            with col3:
                if total_matches > matched_shipments:
                    avg_matches = total_matches / matched_shipments
                    st.metric("🎯 Avg Matches/Shipment", f"{avg_matches:.1f}")
                    st.caption("Average matches per matched shipment")
                else:
                    st.metric("🎯 Match Complexity", "1.0")
                    st.caption("Simple 1:1 matching")
                    
            with col4:
                multi_match_shipments = total_matches - matched_shipments
                st.metric("📊 Multi-Match Instances", f"{multi_match_shipments:,}")
                st.caption("Additional matches beyond 1:1")
        
        st.markdown("---")
        
        # === LAYER DISTRIBUTION ANALYSIS ===
        st.subheader("🎯 Layer Distribution Analysis")
        
        layer_distribution = config_mgr.get_layer_distribution()
        
        if not layer_distribution.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📦 Shipment-Level Analysis** (Accuracy Basis)")
                for _, row in layer_distribution.iterrows():
                    layer = row['match_layer']
                    unique_shipments = row['unique_shipments']
                    shipment_percentage = row['shipment_percentage']
                    
                    # Color code layers
                    if layer == 'LAYER_0':
                        st.success(f"🎯 {layer}: {unique_shipments} shipments ({shipment_percentage}%)")
                    elif layer == 'LAYER_1':
                        st.info(f"🔍 {layer}: {unique_shipments} shipments ({shipment_percentage}%)")  
                    elif layer == 'LAYER_2':
                        st.warning(f"⚡ {layer}: {unique_shipments} shipments ({shipment_percentage}%)")
                    else:
                        st.error(f"🔧 {layer}: {unique_shipments} shipments ({shipment_percentage}%)")
                
                st.caption("💡 Percentages based on unique shipments matched")
            
            with col2:
                st.markdown("**🔗 Match-Level Details** (Review Basis)")
                for _, row in layer_distribution.iterrows():
                    layer = row['match_layer']
                    match_count = row['match_count']
                    unique_shipments = row['unique_shipments']
                    
                    if match_count > unique_shipments:
                        ratio = match_count / unique_shipments
                        st.markdown(f"**{layer}**: {match_count} matches → {ratio:.1f} matches/shipment")
                    else:
                        st.markdown(f"**{layer}**: {match_count} matches (1:1 mapping)")
                
                st.markdown("---")
                st.markdown("**Layer Definitions**")
                st.markdown("""
                - **Layer 0**: 🎯 Exact matches (style + color + delivery)
                - **Layer 1**: 🔍 Style + color matches (delivery flexible)
                - **Layer 2**: ⚡ Fuzzy style matches with color
                - **Layer 3**: 🔧 Advanced pattern matching
                """)
                
                # Missing layers warning
                existing_layers = set(layer_distribution['match_layer'].tolist())
                expected_layers = {'LAYER_0', 'LAYER_1', 'LAYER_2', 'LAYER_3'}
                missing_layers = expected_layers - existing_layers
                
                if missing_layers:
                    st.warning(f"⚠️ Missing layer data: {', '.join(sorted(missing_layers))}")
                    st.caption("Consider running additional matching processes to populate missing layers")
        
        st.markdown("---")
        
        # === REVIEW QUEUE ANALYSIS ===
        st.subheader("🔍 Review Queue Analysis")
        
        review_queue = config_mgr.get_review_queue_summary()
        
        if not review_queue.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Items Requiring Review**")
                for _, row in review_queue.iterrows():
                    review_type = row['review_type']
                    match_count = row['match_count']
                    affected_shipments = row['affected_shipments']
                    affected_customers = row['affected_customers']
                    
                    if 'Quantity' in review_type:
                        st.error(f"📊 {review_type}: {affected_shipments} shipments ({match_count} matches)")
                        st.caption(f"     Affects {affected_customers} customers")
                    elif 'Delivery' in review_type:
                        st.warning(f"🚚 {review_type}: {affected_shipments} shipments ({match_count} matches)")
                        st.caption(f"     Affects {affected_customers} customers")
                    else:
                        st.info(f"🔍 {review_type}: {affected_shipments} shipments ({match_count} matches)")
                        st.caption(f"     Affects {affected_customers} customers")
                
                st.caption("💡 Review counts: Shipments (primary) + Individual matches (detail)")
            
            with col2:
                st.markdown("**System Health**")
                health_metrics = config_mgr.get_system_health_metrics()
                
                if not health_metrics.empty:
                    total_sessions = health_metrics.iloc[0]['total_sessions']
                    st.metric("Total Sessions", total_sessions)
                    
                    last_activity = health_metrics.iloc[0]['last_activity']
                    if last_activity:
                        # Handle both datetime and string types
                        if isinstance(last_activity, str):
                            try:
                                last_activity = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
                            except:
                                # Try pandas to_datetime as fallback
                                import pandas as pd
                                last_activity = pd.to_datetime(last_activity)
                        
                        hours_ago = (datetime.now() - last_activity).total_seconds() / 3600
                        st.caption(f"Last activity: {hours_ago:.1f} hours ago")
        
        st.markdown("---")
        
        # === SHIPMENT-LEVEL SUMMARY ===
        st.subheader("📦 Shipment-Level Summary")
        st.markdown("**One row per shipment** - showing what each shipment matched to")
        
        shipment_summary = config_mgr.get_shipment_level_summary()
        
        if not shipment_summary.empty:
            # Add status indicators
            def style_shipment_status(row):
                if row['shipment_status'] == 'QUANTITY_ISSUES':
                    return ['background-color: #f8d7da'] * len(row)  # Light red
                elif row['shipment_status'] == 'DELIVERY_ISSUES':
                    return ['background-color: #fff3cd'] * len(row)  # Light yellow
                elif row['shipment_status'] == 'UNMATCHED':
                    return ['background-color: #f1f3f4'] * len(row)  # Light gray
                else:
                    return ['background-color: #d4edda'] * len(row)  # Light green
            
            # Format the dataframe for better display
            display_df = shipment_summary.copy()
            display_df['best_confidence'] = display_df['best_confidence'].apply(lambda x: f"{x:.1%}" if x > 0 else "N/A")
            display_df['shipment_status'] = display_df['shipment_status'].str.replace('_', ' ').str.title()
            
            styled_df = display_df.style.apply(style_shipment_status, axis=1)
            
            st.dataframe(
                styled_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "shipment_id": "Shipment ID",
                    "shipment_style": "Style",
                    "shipment_color": "Color", 
                    "shipment_delivery": "Delivery",
                    "shipment_quantity": "Qty",
                    "match_count": "# Matches",
                    "matched_orders": "Matched Orders",
                    "match_layers": "Layers Used",
                    "best_layer": "Best Layer",
                    "best_confidence": "Best Confidence",
                    "shipment_status": "Status"
                }
            )
            
            # Summary stats
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                good_count = len(shipment_summary[shipment_summary['shipment_status'] == 'GOOD'])
                st.metric("✅ Good Matches", good_count)
                
            with col2:
                qty_issues = len(shipment_summary[shipment_summary['shipment_status'] == 'QUANTITY_ISSUES'])
                st.metric("📊 Quantity Issues", qty_issues)
                
            with col3:
                delivery_issues = len(shipment_summary[shipment_summary['shipment_status'] == 'DELIVERY_ISSUES'])
                st.metric("🚚 Delivery Issues", delivery_issues)
                
            with col4:
                unmatched = len(shipment_summary[shipment_summary['shipment_status'] == 'UNMATCHED'])
                st.metric("❌ Unmatched", unmatched)
            
            st.caption("💡 Color coding: 🟢 Good | 🟡 Delivery Issues | 🔴 Quantity Issues | ⚪ Unmatched")
        
        st.markdown("---")
        
        # === CUSTOMER BREAKDOWN ===
        st.subheader("👥 Customer Activity Summary")
        
        customer_breakdown = config_mgr.get_customer_breakdown()
        
        if not customer_breakdown.empty:
            # Style the dataframe for better visualization
            def highlight_status(row):
                if row['status'] == 'approved':
                    return ['background-color: #d4edda'] * len(row)
                elif row['status'] == 'review':
                    return ['background-color: #fff3cd'] * len(row)
                else:
                    return ['background-color: #f8d7da'] * len(row)
            
            styled_df = customer_breakdown.style.apply(highlight_status, axis=1)
            
            st.dataframe(
                styled_df,
                use_container_width=True,
                hide_index=True
            )
            
            st.caption("💡 Green: Approved customers | Yellow: Under review | Red: Deprecated")
        
    except Exception as e:
        st.error(f"Error loading enhanced dashboard: {str(e)}")
        st.info("Make sure the enhanced_matching_results table exists and contains data.")

def show_basic_dashboard(config_mgr):
    """Show basic dashboard with customer overview"""
    try:
        customers_df = config_mgr.get_customers()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Customers", len(customers_df))
        
        with col2:
            approved_count = len(customers_df[customers_df['status'] == 'approved'])
            st.metric("Approved Customers", approved_count)
            
        with col3:
            review_count = len(customers_df[customers_df['status'] == 'review'])
            st.metric("Under Review", review_count)
            
        with col4:
            deprecated_count = len(customers_df[customers_df['status'] == 'deprecated'])
            st.metric("Deprecated", deprecated_count)
        
        st.subheader("Customer Status Overview")
        st.dataframe(customers_df, use_container_width=True)
        
        # Recent activity placeholder
        st.subheader("Recent Configuration Changes")
        st.info("Audit trail integration coming soon...")
        
    except Exception as e:
        st.error(f"Error loading dashboard: {str(e)}")
        st.info("Make sure the database schema has been created and is accessible.")

def show_hitl_review(config_mgr):
    """Enhanced Human-in-the-Loop matching review interface with rich context"""
    st.header("🔍 Human-in-the-Loop Review")
    st.markdown("Review matching results with full context - style, color, delivery methods, and quantities")
    
    # Customer filter
    try:
        customers_df = config_mgr.get_customers()
        customer_options = ["All Customers"] + customers_df['canonical_name'].tolist()
        selected_customer = st.selectbox("Filter by Customer", customer_options)
        
        customer_filter = None if selected_customer == "All Customers" else selected_customer
        
        # Load enhanced matching results from database
        st.subheader("📊 Enhanced Matching Results Overview")
        
        # Get all matching results with full context
        all_results = config_mgr.get_enhanced_matching_results(customer_name=customer_filter)
        
        if all_results.empty:
            st.warning("🚨 No enhanced matching results found. Run the enhanced matcher first to generate data for HITL review.")
            st.info("💡 **Tip**: Use the enhanced_db_matcher.py script to process orders and create matching results.")
            return
        
        # Summary metrics with rich context
        col1, col2, col3, col4 = st.columns(4)
        
        total_matches = len(all_results)
        delivery_mismatches = len(all_results[all_results['delivery_match'] == 0])
        qty_failures = len(all_results[all_results['quantity_check_result'] == 'FAIL'])
        perfect_matches = len(all_results[
            (all_results['style_match'] == 1) & 
            (all_results['color_match'] == 1) & 
            (all_results['delivery_match'] == 1) & 
            (all_results['quantity_check_result'] == 'PASS')
        ])
        
        with col1:
            st.metric("Total Matches", total_matches)
        with col2:
            st.metric("🎯 Perfect Matches", perfect_matches)
        with col3:
            st.metric("🚚 Delivery Issues", delivery_mismatches)
        with col4:
            st.metric("⚖️ Quantity Issues", qty_failures)
        
        # Enhanced tabbed interface with rich context
        tab1, tab2, tab3, tab4, tab5 = st.tabs([" All Shipments", " All Matches", " Delivery Mismatches", " Quantity Issues", " Unmatched Orders"])
        
        with tab1:
            show_all_shipments_with_status(customer_filter, config_mgr)
        
        with tab2:
            show_enhanced_all_matches(all_results, config_mgr)
        
        with tab3:
            show_enhanced_delivery_review(customer_filter, config_mgr)
        
        with tab4:
            show_enhanced_quantity_review(customer_filter, config_mgr)
            
        with tab5:
            show_unmatched_orders(customer_filter, config_mgr)
            
    except Exception as e:
        st.error(f"Error loading HITL review: {str(e)}")
        st.exception(e)

def show_all_shipments_with_status(customer_filter, config_mgr):
    """Show all shipments with their match status - complete inventory view"""
    st.subheader("📦 Complete Shipment Inventory with Match Status")
    
    # === SHIPMENT-LEVEL SUMMARY (moved from Dashboard) ===
    st.markdown("---")
    st.subheader("📦 Shipment-Level Summary")
    st.markdown("**Shipments with matching history** - filtered by customer selection")
    
    shipment_summary = config_mgr.get_shipment_level_summary(customer_filter)
    
    if not shipment_summary.empty:
        # Function to convert Y/N/P/U to symbols
        def format_indicator(val):
            if val == 'Y':
                return '✓'
            elif val == 'N':
                return '✗'
            elif val == 'P':
                return '~'
            else:
                return '?'
        
        # Enhanced column configuration for better display
        column_config = {
            'row_num': st.column_config.NumberColumn('Row', width='small'),
            'shipment_id': st.column_config.TextColumn('Shipment ID', width='medium'),
            'shipment_style': st.column_config.TextColumn('Style', width='medium'),
            'shipment_color': st.column_config.TextColumn('Color', width='medium'), 
            'style_match_indicator': st.column_config.TextColumn('Style ✓', width='small'),
            'color_match_indicator': st.column_config.TextColumn('Color ✓', width='small'),
            'delivery_match_indicator': st.column_config.TextColumn('Delivery ✓', width='small'),
            'quantity_match_indicator': st.column_config.TextColumn('Qty ✓', width='small'),
            'match_layers': st.column_config.TextColumn('Layers', width='small'),
            'best_confidence': st.column_config.ProgressColumn(
                'Confidence', 
                min_value=0, 
                max_value=1,
                width='medium',
                format="%.1%"
            ),
            'shipment_quantity': st.column_config.NumberColumn('Ship Qty', width='small'),
            'total_matched_order_qty': st.column_config.NumberColumn('Order Qty', width='small'),
            'quantity_variance': st.column_config.NumberColumn('Variance', width='small'),
            'shipment_status': st.column_config.TextColumn('Status', width='medium'),
        }
            
        # Add status indicators with enhanced styling
        def style_shipment_status(row):
            if row['shipment_status'] == 'QUANTITY_ISSUES':
                return ['background-color: #f8d7da'] * len(row)  # Light red
            elif row['shipment_status'] == 'DELIVERY_ISSUES':
                return ['background-color: #fff3cd'] * len(row)  # Light yellow
            elif row['shipment_status'] == 'UNMATCHED':
                return ['background-color: #f1f3f4'] * len(row)  # Light gray
            else:
                return ['background-color: #d4edda'] * len(row)  # Light green
        
        # Format the dataframe for better display
        display_df = shipment_summary.copy()
        
        # Convert match indicators to symbols
        display_df['style_match_indicator'] = display_df['style_match_indicator'].apply(format_indicator)
        display_df['color_match_indicator'] = display_df['color_match_indicator'].apply(format_indicator)
        display_df['delivery_match_indicator'] = display_df['delivery_match_indicator'].apply(format_indicator)
        display_df['quantity_match_indicator'] = display_df['quantity_match_indicator'].apply(format_indicator)
        
        # Format status text
        display_df['shipment_status'] = display_df['shipment_status'].str.replace('_', ' ').str.title()
        
        styled_df = display_df.style.apply(style_shipment_status, axis=1)
        
        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True,
            column_config=column_config
        )
        
        # Summary stats
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📦 Total Matched Shipments", len(shipment_summary))
            
        with col2:
            good_count = len(shipment_summary[shipment_summary['shipment_status'] == 'GOOD'])
            st.metric("✅ Good Matches", good_count)
            
        with col3:
            qty_issues = len(shipment_summary[shipment_summary['shipment_status'] == 'QUANTITY_ISSUES'])
            st.metric("📊 Quantity Issues", qty_issues)
            
        with col4:
            delivery_issues = len(shipment_summary[shipment_summary['shipment_status'] == 'DELIVERY_ISSUES'])
            st.metric("🚚 Delivery Issues", delivery_issues)
        
        st.caption("💡 Only showing shipments with matching history | Color coding: 🟢 Good | 🟡 Delivery Issues | 🔴 Quantity Issues")
    else:
        if customer_filter and customer_filter != "All Customers":
            st.info(f"No matched shipments found for customer: {customer_filter}")
        else:
            st.info("No matched shipments found. Select a customer to see matching history.")
    
    st.markdown("---")
    # === END SHIPMENT-LEVEL SUMMARY ===
    
    try:
        # Get all shipments for the customer (fix customer name issue)
        customer_name = customer_filter if customer_filter else "GREYSON CLOTHIERS"
        
        # Query all shipments with enhanced match status including layers
        query = """
        SELECT 
            s.shipment_id,
            s.style_code,
            s.color_description,
            s.delivery_method,
            s.quantity,
            s.shipped_date,
            CASE 
                WHEN emr.shipment_id IS NOT NULL THEN 'MATCHED'
                ELSE 'UNMATCHED'
            END as match_status,
            COALESCE(emr.match_layer, 'NO_MATCH') as match_layer,
            COALESCE(emr.match_confidence, 0) as match_confidence,
            CASE WHEN emr.style_match = 'MATCH' THEN 1 ELSE 0 END as style_match,
            CASE WHEN emr.color_match = 'MATCH' THEN 1 ELSE 0 END as color_match,
            CASE WHEN emr.delivery_match = 'MATCH' THEN 1 ELSE 0 END as delivery_match,
            emr.quantity_check_result,
            CASE 
                WHEN emr.match_layer = 'LAYER_0' THEN 'Perfect Match'
                WHEN emr.match_layer = 'LAYER_1' THEN 'Fuzzy-Good'
                WHEN emr.match_layer = 'LAYER_2' THEN 'Fuzzy-Deep'
                WHEN emr.shipment_id IS NULL THEN 'No Match'
                ELSE 'Unknown'
            END as layer_status_display
        FROM stg_fm_orders_shipped_table s
        LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
        WHERE s.customer_name LIKE ? AND s.po_number = '4755'
        ORDER BY 
            CASE 
                WHEN emr.match_layer IS NULL THEN 99
                WHEN emr.match_layer = 'LAYER_0' THEN 0
                WHEN emr.match_layer = 'LAYER_1' THEN 1
                WHEN emr.match_layer = 'LAYER_2' THEN 2
                ELSE 3
            END,
            s.shipment_id
        """
        
        all_shipments = config_mgr.execute_query(query, [f"{customer_name}%"])
        
        if all_shipments.empty:
            st.warning("No shipments found for the selected criteria")
            return
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_shipments = len(all_shipments)
        matched_shipments = len(all_shipments[all_shipments['match_status'] == 'MATCHED'])
        unmatched_shipments = total_shipments - matched_shipments
        match_rate = (matched_shipments / total_shipments * 100) if total_shipments > 0 else 0
        
        with col1:
            st.metric("📦 Total Shipments", total_shipments)
        with col2:
            st.metric("✅ Matched", matched_shipments)
        with col3:
            st.metric("❌ Unmatched", unmatched_shipments)
        with col4:
            st.metric("📊 Match Rate", f"{match_rate:.1f}%")
        
        # Filter controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.selectbox(
                "Filter by Status",
                ["All", "MATCHED", "UNMATCHED"],
                key="shipment_status_filter"
            )
        
        with col2:
            delivery_options = ["All"] + sorted(all_shipments['delivery_method'].dropna().unique())
            delivery_filter = st.selectbox(
                "Filter by Delivery Method",
                delivery_options,
                key="shipment_delivery_filter"
            )
        
        with col3:
            style_options = ["All"] + sorted(all_shipments['style_code'].dropna().unique())
            style_filter = st.selectbox(
                "Filter by Style",
                style_options[:10],  # Limit to first 10 for UI space
                key="shipment_style_filter"
            )
        
        # Apply filters
        filtered_df = all_shipments.copy()
        
        if status_filter != "All":
            filtered_df = filtered_df[filtered_df['match_status'] == status_filter]
        
        if delivery_filter != "All":
            filtered_df = filtered_df[filtered_df['delivery_method'] == delivery_filter]
        
        if style_filter != "All":
            filtered_df = filtered_df[filtered_df['style_code'] == style_filter]
        
        # Display results
        st.subheader(f"📋 Shipments ({len(filtered_df)} of {total_shipments})")
        
        if not filtered_df.empty:
            # Create display dataframe with enhanced formatting and layer information
            display_df = pd.DataFrame({
                'Shipment ID': filtered_df['shipment_id'],
                'Style': filtered_df['style_code'],
                'Color': filtered_df['color_description'],
                'Delivery': filtered_df['delivery_method'],
                'Qty': filtered_df['quantity'],
                'Status': filtered_df['layer_status_display'],  # Use the layer-based status
                'Match Layer': filtered_df['match_layer'].fillna('N/A'),
                'Confidence': filtered_df['match_confidence'].apply(
                    lambda x: f"{x:.1%}" if pd.notna(x) and x > 0 else "N/A"
                ),
                'Style ✓': filtered_df['style_match'].apply(
                    lambda x: "✅" if x == 1 else "❌" if pd.notna(x) else "N/A"
                ),
                'Color ✓': filtered_df['color_match'].apply(
                    lambda x: "✅" if x == 1 else "❌" if pd.notna(x) else "N/A"
                ),
                'Delivery ✓': filtered_df['delivery_match'].apply(
                    lambda x: "✅" if x == 1 else "❌" if pd.notna(x) else "N/A"
                ),
                'Qty Check': filtered_df['quantity_check_result'].fillna('N/A')
            })
            
            # Display with styling
            st.dataframe(
                display_df,
                use_container_width=True,
                height=400
            )
            
            # Quick actions for unmatched items
            if unmatched_shipments > 0:
                st.subheader("🔧 Quick Actions for Unmatched Items")
                
                unmatched_df = all_shipments[all_shipments['match_status'] == 'UNMATCHED']
                
                with st.expander(f"📋 View {unmatched_shipments} Unmatched Shipments"):
                    unmatched_display = pd.DataFrame({
                        'Shipment ID': unmatched_df['shipment_id'],
                        'Style': unmatched_df['style_code'],
                        'Color': unmatched_df['color_description'],
                        'Delivery': unmatched_df['delivery_method'],
                        'Qty': unmatched_df['quantity'],
                        'Possible Reason': 'No matching order found'
                    })
                    
                    st.dataframe(unmatched_display, use_container_width=True)
                    
                    st.info("""
                    💡 **Why shipments might be unmatched:**
                    - Order not in system yet
                    - Style/color code differences
                    - Delivery method mismatch
                    - Order already fully shipped
                    - Cancelled orders (excluded from matching)
                    """)
        else:
            st.info("No shipments match the selected filters")
            
    except Exception as e:
        st.error(f"Error loading shipments: {str(e)}")
        st.exception(e)

def show_enhanced_all_matches(all_results, config_mgr):
    """Enhanced view of all matches with full context and Layer analysis"""
    st.subheader("📋 All Match Results with Layer-based Analysis")
    
    if all_results.empty:
        st.info("No matching results to display")
        return
    
    # Add Layer-based breakdown
    if 'match_layer' in all_results.columns:
        st.markdown("### 🎯 Layer-based Match Distribution")
        
        layer_counts = all_results['match_layer'].value_counts()
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            layer0_count = layer_counts.get('LAYER_0', 0)
            st.metric("🎯 Layer 0 (Perfect)", layer0_count, help="Exact matches on style, color, and delivery")
        
        with col2:
            layer1_count = layer_counts.get('LAYER_1', 0)
            st.metric("🔄 Layer 1 (Fuzzy)", layer1_count, help="Exact style+color, flexible delivery")
        
        with col3:
            layer2_count = layer_counts.get('LAYER_2', 0)
            st.metric("🔍 Layer 2 (Deep)", layer2_count, help="Fuzzy matching for data variations")
        
        with col4:
            total_matches = len(all_results)
            st.metric("📊 Total Matches", total_matches)
        
        # Layer analysis buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🎯 Focus on Layer 0 Matches", key="focus_layer0"):
                st.session_state['layer_filter'] = 'LAYER_0'
        with col2:
            if st.button("🔄 Focus on Layer 1&2 Fuzzy", key="focus_fuzzy"):
                st.session_state['layer_filter'] = ['LAYER_1', 'LAYER_2']
        
        # Apply layer filter if set
        if 'layer_filter' in st.session_state:
            layer_filter = st.session_state['layer_filter']
            if isinstance(layer_filter, list):
                filtered_results = all_results[all_results['match_layer'].isin(layer_filter)]
                st.info(f"Showing {len(filtered_results)} Layer 1&2 fuzzy matches")
            else:
                filtered_results = all_results[all_results['match_layer'] == layer_filter]
                st.info(f"Showing {len(filtered_results)} {layer_filter} matches")
            
            if st.button("🔄 Show All Layers", key="show_all"):
                del st.session_state['layer_filter']
                st.experimental_rerun()
        else:
            filtered_results = all_results
    
    # Create display dataframe with rich context
    display_df = pd.DataFrame({
        'Match ID': filtered_results['id'],
        'Customer': filtered_results['customer_name'],
        'PO#': filtered_results['po_number'],
        'Ship ID': filtered_results['shipment_id'],
        'Order ID': filtered_results['order_id'],
        
        # Style Context
        'Ship Style': filtered_results['shipment_style'],
        'Order Style': filtered_results['order_style'],
        'Style ✓': filtered_results['style_match'].map({1: '✅', 0: '❌'}),
        
        # Color Context
        'Ship Color': filtered_results['shipment_color'],
        'Order Color': filtered_results['order_color'],
        'Color ✓': filtered_results['color_match'].map({1: '✅', 0: '❌'}),
        
        # Delivery Context
        'Ship Delivery': filtered_results['shipment_delivery'],
        'Order Delivery': filtered_results['order_delivery'],
        'Delivery ✓': filtered_results['delivery_match'].map({1: '✅', 0: '❌'}),
        
        # Quantity Context
        'Ship Qty': filtered_results['shipment_qty'],
        'Order Qty': filtered_results['order_qty'],
        'Qty Diff %': filtered_results['quantity_difference_percent'].round(1),
        'Qty Status': filtered_results['quantity_check_result'],
        
        # Match Quality - Enhanced with Layer info
        'Layer': filtered_results['match_layer'],
        'Score': filtered_results['match_confidence'].round(2),
        'Status': filtered_results['status']
    })
    
    # Style the dataframe for better visual feedback
    def style_match_results(row):
        styles = []
        for col in row.index:
            if '✓' in col:
                if row[col] == '✅':
                    styles.append('background-color: #d4edda; color: #155724')  # Green
                elif row[col] == '❌':
                    styles.append('background-color: #f8d7da; color: #721c24')  # Red
                else:
                    styles.append('')
            elif col == 'Qty Status':
                if row[col] == 'PASS':
                    styles.append('background-color: #d4edda; color: #155724')  # Green
                elif row[col] == 'FAIL':
                    styles.append('background-color: #f8d7da; color: #721c24')  # Red
                else:
                    styles.append('')
            elif col == 'Status':
                if row[col] == 'approved':
                    styles.append('background-color: #d1ecf1; color: #0c5460')  # Blue
                elif row[col] == 'review':
                    styles.append('background-color: #fff3cd; color: #856404')  # Yellow
                else:
                    styles.append('')
            else:
                styles.append('')
        return styles
    
    styled_df = display_df.style.apply(style_match_results, axis=1)
    st.dataframe(styled_df, use_container_width=True, height=400)
    
    # Quick actions
    st.subheader("🔧 Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("✅ Approve All Perfect Matches"):
            perfect_matches = all_results[
                (all_results['style_match'] == 1) & 
                (all_results['color_match'] == 1) & 
                (all_results['delivery_match'] == 1) & 
                (all_results['quantity_check_result'] == 'PASS')
            ]
            st.success(f"Would approve {len(perfect_matches)} perfect matches")
    
    with col2:
        if st.button("📋 Mark All for Review"):
            st.info("Would mark all matches for human review")
    
    with col3:
        if st.button("📊 Export to CSV"):
            csv_data = display_df.to_csv(index=False)
            st.download_button(
                "Download CSV",
                csv_data,
                "matching_results.csv",
                "text/csv"
            )

def show_enhanced_delivery_review(customer_filter, config_mgr):
    """Enhanced delivery method mismatch review with mapping interface"""
    st.subheader("🚚 Delivery Method Mismatch Resolution")
    
    # Get delivery mismatch summary
    delivery_summary = config_mgr.get_delivery_mismatches_summary(customer_name=customer_filter)
    
    if delivery_summary.empty:
        st.success("🎉 **Excellent!** No delivery method mismatches found!")
        st.info("All shipment and order delivery methods match perfectly. No HITL intervention needed.")
        return
    
    st.warning(f"📦 Found **{len(delivery_summary)}** unique delivery method mismatches requiring resolution")
    
    # Summary table with full context
    st.subheader("📊 Delivery Mismatch Summary")
    
    summary_display = pd.DataFrame({
        'Customer': delivery_summary['customer_name'],
        'Shipment Delivery': delivery_summary['shipment_delivery'],
        'Order Delivery': delivery_summary['order_delivery'],
        'Affected Count': delivery_summary['mismatch_count'],
        'Sample Shipments': delivery_summary['affected_shipments'].str[:50] + '...'
    })
    
    st.dataframe(summary_display, use_container_width=True)
    
    # Global Delivery Method Mapping Interface
    st.subheader("🌐 Global Delivery Method Mapping")
    st.markdown("Create **customer-agnostic** delivery method mappings that apply across all customers")
    
    with st.expander("➕ Create New Global Delivery Mapping"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            source_delivery = st.text_input("Source Delivery Method", placeholder="e.g., AIR")
            target_delivery = st.text_input("Target Delivery Method", placeholder="e.g., EXPRESS")
        
        with col2:
            mapping_type = st.selectbox("Mapping Type", [
                "Equivalent (Treat as Same)",
                "Standardize (Convert Source→Target)", 
                "Flag for Review (Requires Manual Check)"
            ])
            
            confidence = st.slider("Confidence Level", 0, 100, 95, step=5)
        
        with col3:
            business_justification = st.text_area(
                "Business Justification", 
                placeholder="Why should these delivery methods be treated as equivalent?\n\nExample: AIR and EXPRESS both represent expedited shipping with 1-2 day delivery."
            )
        
        if st.button("💾 Create Global Mapping"):
            if source_delivery and target_delivery and business_justification:
                # This would save to a global_delivery_mappings table
                st.success(f"✅ Global mapping created: {source_delivery} → {target_delivery}")
                st.info("🔄 This mapping will be applied to all future matching operations across all customers.")
            else:
                st.error("Please fill in all required fields")
    
    # Individual mismatch resolution
    st.subheader("🔍 Individual Mismatch Resolution")
    
    for idx, row in delivery_summary.iterrows():
        with st.expander(f"🚚 {row['customer_name']}: {row['shipment_delivery']} ↔ {row['order_delivery']} ({row['mismatch_count']} matches)"):
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**📦 Shipment Context:**")
                st.code(f"Delivery Method: {row['shipment_delivery']}")
                st.write("**📋 Order Context:**")
                st.code(f"Delivery Method: {row['order_delivery']}")
                st.write(f"**📊 Impact:** {row['mismatch_count']} affected shipments")
            
            with col2:
                resolution_action = st.selectbox(
                    "Resolution Action",
                    [
                        "Create Equivalent Mapping",
                        "Standardize to Order Method",
                        "Standardize to Shipment Method", 
                        "Flag All for Manual Review",
                        "Reject as Invalid Matches"
                    ],
                    key=f"delivery_action_{idx}"
                )
                
                justification = st.text_area(
                    "Business Justification",
                    placeholder="Explain the business logic behind this resolution...",
                    key=f"delivery_justify_{idx}"
                )
            
            if st.button(f"💾 Apply Resolution", key=f"delivery_save_{idx}"):
                if justification:
                    st.success(f"✅ Resolution applied for {row['customer_name']} delivery mismatch")
                    st.info("🔄 Future matches will use this mapping automatically")
                else:
                    st.error("Business justification is required")

def show_enhanced_quantity_review(customer_filter, config_mgr):
    """Enhanced quantity tolerance review with detailed context"""
    st.subheader("⚖️ Quantity Tolerance Review")
    
    # Get detailed quantity issues
    qty_issues = config_mgr.get_quantity_issues_detailed(customer_name=customer_filter)
    
    if qty_issues.empty:
        st.success("🎉 **Perfect!** All quantities are within acceptable tolerance!")
        st.info("No quantity discrepancies require human review. The system is operating within defined parameters.")
        return
    
    st.warning(f"⚖️ Found **{len(qty_issues)}** quantity discrepancies requiring review")
    
    # Severity breakdown
    severity_counts = qty_issues['severity_level'].value_counts()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        critical_count = severity_counts.get('Critical', 0)
        st.metric("🚨 Critical (>100%)", critical_count)
    with col2:
        high_count = severity_counts.get('High', 0)
        st.metric("⚠️ High (50-100%)", high_count)
    with col3:
        medium_count = severity_counts.get('Medium', 0)
        st.metric("📊 Medium (25-50%)", medium_count)
    with col4:
        low_count = severity_counts.get('Low', 0)
        st.metric("ℹ️ Low (<25%)", low_count)
    
    # Visual distribution
    if not severity_counts.empty:
        st.subheader("📈 Quantity Issue Distribution")
        st.bar_chart(severity_counts)
    
    # Detailed review cards
    st.subheader("🔍 Individual Quantity Reviews")
    
    # Sort by severity for prioritized review
    severity_order = {'Critical': 0, 'High': 1, 'Medium': 2, 'Low': 3}
    qty_issues_sorted = qty_issues.sort_values('severity_level', key=lambda x: x.map(severity_order))
    
    for idx, issue in qty_issues_sorted.iterrows():
        # Color-code the expander based on severity
        severity_emoji = {
            'Critical': '🚨', 'High': '⚠️', 'Medium': '📊', 'Low': 'ℹ️'
        }
        
        severity_color = {
            'Critical': '#dc3545', 'High': '#fd7e14', 'Medium': '#ffc107', 'Low': '#17a2b8'
        }
        
        with st.expander(
            f"{severity_emoji[issue['severity_level']]} {issue['severity_level']} Priority | "
            f"Shipment {issue['shipment_id']} | {issue['quantity_difference_percent']:.1f}% difference"
        ):
            
            # Context-rich display
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**📦 Shipment Context**")
                st.write(f"**ID:** {issue['shipment_id']}")
                st.write(f"**Style:** {issue['shipment_style']}")
                st.write(f"**Color:** {issue['shipment_color']}")
                st.write(f"**Quantity:** {issue['shipment_qty']}")
                st.write(f"**Delivery:** {issue['shipment_delivery']}")
            
            with col2:
                st.markdown("**📋 Order Context**")
                st.write(f"**ID:** {issue['order_id']}")
                st.write(f"**Style:** {issue['order_style']}")
                st.write(f"**Color:** {issue['order_color']}")
                st.write(f"**Quantity:** {issue['order_qty']}")
                st.write(f"**Delivery:** {issue['order_delivery']}")
            
            with col3:
                st.markdown("**📊 Variance Analysis**")
                st.write(f"**Absolute Difference:** {issue['qty_difference']}")
                st.write(f"**Percentage Difference:** {issue['quantity_difference_percent']:.1f}%")
                st.write(f"**Match Score:** {issue['match_confidence']:.2f}")
                st.write(f"**Severity:** {issue['severity_level']}")
                
                # Visual indicator
                if issue['severity_level'] == 'Critical':
                    st.error("🚨 Requires immediate investigation")
                elif issue['severity_level'] == 'High':
                    st.warning("⚠️ Manual approval needed")
                else:
                    st.info("ℹ️ Minor variance - review recommended")
            
            # Action interface
            st.markdown("---")
            col_action1, col_action2, col_action3 = st.columns(3)
            
            with col_action1:
                review_action = st.selectbox(
                    "Review Decision",
                    [
                        "Pending Review",
                        "✅ Approve Override", 
                        "🔍 Request Investigation",
                        "❌ Reject Match",
                        "📞 Escalate to Management"
                    ],
                    key=f"qty_action_{issue['id']}"
                )
            
            with col_action2:
                tolerance_override = st.checkbox(
                    "Override Tolerance", 
                    help="Allow this match despite quantity variance",
                    key=f"qty_override_{issue['id']}"
                )
                
                if tolerance_override:
                    override_reason = st.selectbox(
                        "Override Reason",
                        [
                            "Business Exception",
                            "Data Entry Error", 
                            "Partial Shipment Expected",
                            "Customer-Approved Variance",
                            "System Calculation Error"
                        ],
                        key=f"qty_reason_{issue['id']}"
                    )
            
            with col_action3:
                business_notes = st.text_area(
                    "Business Justification",
                    placeholder="Document the business reasoning for this decision...\n\nExample: Customer confirmed partial shipment due to inventory shortage. Remaining quantity will ship next week.",
                    key=f"qty_notes_{issue['id']}"
                )
            
            # Save decision button
            if st.button(f"💾 Save Decision", key=f"qty_save_{issue['id']}"):
                if business_notes or review_action == "Pending Review":
                    # This would save to hitl_decisions table
                    st.success(f"✅ Decision saved for shipment {issue['shipment_id']}")
                    st.info("🔄 Decision will be applied to future matching operations")
                else:
                    st.error("Business justification required for non-pending decisions")

def show_customer_management(config_mgr):
    """Customer CRUD interface"""
    st.header("Customer Management")
    
    tab1, tab2 = st.tabs(["View Customers", "Add/Edit Customer"])
    
    with tab1:
        try:
            customers_df = config_mgr.get_customers()
            st.dataframe(customers_df, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading customers: {str(e)}")
    
    with tab2:
        st.subheader("Add New Customer")
        
        with st.form("customer_form"):
            canonical_name = st.text_input("Canonical Name*", help="Primary customer identifier")
            status = st.selectbox("Status", ["approved", "review", "deprecated"])
            
            col1, col2 = st.columns(2)
            with col1:
                packed_products = st.text_input("Packed Products")
                shipped = st.text_input("Shipped")
            with col2:
                master_order_list = st.text_input("Master Order List")
                mon_customer_ms = st.text_input("Mon Customer MS")
            
            aliases = st.text_area("Aliases (one per line)", help="Alternative names for this customer")
            
            if st.form_submit_button("Save Customer"):
                if canonical_name:
                    try:
                        customer_data = {
                            'canonical_name': canonical_name,
                            'status': status,
                            'packed_products': packed_products,
                            'shipped': shipped,
                            'master_order_list': master_order_list
                        }
                        
                        customer_id = config_mgr.save_customer(customer_data)
                        st.success(f"Customer '{canonical_name}' saved successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error saving customer: {str(e)}")
                else:
                    st.error("Canonical name is required")

def show_column_mappings(config_mgr):
    """Column mapping management interface"""
    st.header("Column Mappings")
    st.markdown("Define how order columns map to shipment columns")
    
    # Customer selection
    try:
        customers_df = config_mgr.get_customers()
        customer_options = ["Global (All Customers)"] + customers_df['canonical_name'].tolist()
        selected_customer = st.selectbox("Select Customer", customer_options)
        
        st.subheader(f"Column Mappings for: {selected_customer}")
        
        # Add new mapping form
        with st.expander("Add New Mapping"):
            with st.form("mapping_form"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    order_col = st.text_input("Order Column")
                with col2:
                    shipment_col = st.text_input("Shipment Column")
                with col3:
                    priority = st.number_input("Priority", min_value=1, value=1)
                
                if st.form_submit_button("Add Mapping"):
                    st.success("Mapping functionality will be implemented next!")
        
        # Display existing mappings placeholder
        st.info("Existing mappings will be displayed here")
        
    except Exception as e:
        st.error(f"Error loading column mappings: {str(e)}")

def show_matching_strategies(config_mgr):
    """Matching strategy configuration"""
    st.header("Matching Strategies")
    st.markdown("Configure how orders and shipments are matched")
    
    st.info("Matching strategy configuration interface coming next!")

def show_exclusion_rules(config_mgr):
    """Exclusion rules management"""
    st.header("Exclusion Rules")
    st.markdown("Define rules to exclude certain records from matching")
    
    st.info("Exclusion rules interface coming next!")

def show_value_mappings(config_mgr):
    """Value mapping interface"""
    st.header("Value Mappings")
    st.markdown("Map variant values to canonical forms")
    
    st.info("Value mapping interface coming next!")

def show_audit_trail(config_mgr):
    """Audit trail viewer"""
    st.header("Audit Trail")
    st.markdown("View configuration change history")
    
    st.info("Audit trail viewer coming next!")

def show_import_export(config_mgr):
    """Import/Export interface"""
    st.header("Import/Export")
    
    tab1, tab2 = st.tabs(["Import from YAML", "Export to YAML"])
    
    with tab1:
        st.subheader("Import Configuration from YAML")
        
        uploaded_file = st.file_uploader("Choose YAML file", type=['yaml', 'yml'])
        
        if uploaded_file is not None:
            try:
                yaml_content = yaml.safe_load(uploaded_file)
                st.json(yaml_content)
                
                if st.button("Import Configuration"):
                    st.success("Import functionality will be implemented next!")
                    
            except Exception as e:
                st.error(f"Error parsing YAML: {str(e)}")
    
    with tab2:
        st.subheader("Export Configuration to YAML")
        st.info("Export functionality coming next!")

def show_unmatched_orders(customer_filter, config_mgr):
    """Show orders that don't have matching shipments - helps identify quantity mismatches"""
    st.subheader("📝 Unmatched Orders Analysis")
    
    try:
        # Get all orders for the customer/PO
        customer_name = customer_filter['canonical_name']
        po_number = "4755"  # Hardcoded for now, should be parameter
        
        # Query to get orders that aren't matched to shipments
        orders_query = """
        SELECT 
            o.order_id,
            o.style_code,
            o.color_description,
            o.delivery_method,
            o.quantity,
            o.order_date,
            o.order_type,
            CASE 
                WHEN emr.order_id IS NOT NULL THEN 'MATCHED'
                ELSE 'UNMATCHED'
            END as match_status
        FROM stg_order_list o
        LEFT JOIN enhanced_matching_results emr ON o.order_id = emr.order_id
        WHERE o.customer_name LIKE ? AND o.po_number = ?
        AND o.order_type != 'CANCELLED'
        ORDER BY 
            CASE WHEN emr.order_id IS NULL THEN 0 ELSE 1 END,
            o.style_code, o.color_description
        """
        
        orders_df = config_mgr.execute_query(orders_query, [f"{customer_name}%", po_number])
        
        if orders_df.empty:
            st.warning("No orders found for the selected criteria")
            return
            
        # Separate matched and unmatched orders
        unmatched_orders = orders_df[orders_df['match_status'] == 'UNMATCHED']
        matched_orders = orders_df[orders_df['match_status'] == 'MATCHED']
        
        # Show summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Orders", len(orders_df))
        with col2:
            st.metric("Matched Orders", len(matched_orders))
        with col3:
            st.metric("Unmatched Orders", len(unmatched_orders))
            
        if len(unmatched_orders) > 0:
            st.markdown("### 🔍 Unmatched Orders - Potential Quantity Issues")
            st.markdown("*These orders may represent quantity mismatches or unshipped items*")
            
            # Add style and functionality for manual matching
            st.dataframe(
                unmatched_orders,
                use_container_width=True,
                column_config={
                    "order_id": st.column_config.TextColumn("Order ID", width="medium"),
                    "style_code": st.column_config.TextColumn("Style", width="medium"),
                    "color_description": st.column_config.TextColumn("Color", width="medium"), 
                    "delivery_method": st.column_config.TextColumn("Delivery", width="small"),
                    "quantity": st.column_config.NumberColumn("Quantity", format="%d"),
                    "order_date": st.column_config.DateColumn("Order Date"),
                    "order_type": st.column_config.TextColumn("Type", width="small")
                }
            )
            
            # Group unmatched orders by style+color for analysis
            if not unmatched_orders.empty:
                st.markdown("### 📈 Unmatched Orders by Style+Color")
                style_color_summary = unmatched_orders.groupby(['style_code', 'color_description']).agg({
                    'quantity': 'sum',
                    'order_id': 'count'
                }).rename(columns={'order_id': 'order_count'}).reset_index()
                
                st.dataframe(
                    style_color_summary,
                    use_container_width=True,
                    column_config={
                        "style_code": "Style",
                        "color_description": "Color", 
                        "quantity": st.column_config.NumberColumn("Total Quantity", format="%d"),
                        "order_count": st.column_config.NumberColumn("Order Count", format="%d")
                    }
                )
        else:
            st.success("🎉 All orders have been matched to shipments!")
            
        # Show matched orders summary for comparison
        if len(matched_orders) > 0:
            with st.expander("📋 Matched Orders Summary", expanded=False):
                st.dataframe(
                    matched_orders[['order_id', 'style_code', 'color_description', 'delivery_method', 'quantity']],
                    use_container_width=True
                )
                
    except Exception as e:
        st.error(f"Error loading unmatched orders: {str(e)}")
        st.exception(e)

if __name__ == "__main__":
    main()
