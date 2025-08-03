"""
ENHANCED Version of Streamlit Configuration Management UI
Enhanced with comprehensive dashboard analytics
Created: August 1, 2025
Purpose: Enhanced dashboard with layer analytics and system metrics
"""

import streamlit as st
import pandas as pd
import pyodbc
import json
from datetime import datetime
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from auth_helper import get_connection_string

class EnhancedConfigurationManager:
    def __init__(self):
        self.connection_string = get_connection_string()
        
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def execute_query(self, query, params=None):
        """Execute a custom query and return results as DataFrame"""
        with self.get_connection() as conn:
            if params:
                return pd.read_sql(query, conn, params=params)
            else:
                return pd.read_sql(query, conn)
    
    # === DASHBOARD ANALYTICS QUERIES ===
    
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
        """Get shipment summary with match status"""
        query = """
        SELECT 
            COUNT(DISTINCT s.shipment_id) as total_shipments,
            COUNT(DISTINCT emr.shipment_id) as matched_shipments,
            COUNT(DISTINCT s.shipment_id) - COUNT(DISTINCT emr.shipment_id) as unmatched_shipments,
            CAST(COUNT(DISTINCT emr.shipment_id) * 100.0 / COUNT(DISTINCT s.shipment_id) AS DECIMAL(5,1)) as match_rate_pct
        FROM stg_fm_orders_shipped_table s
        LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
        """
        return self.execute_query(query)
    
    def get_layer_distribution(self):
        """Get match layer distribution"""
        query = """
        SELECT 
            match_layer,
            COUNT(*) as match_count,
            COUNT(DISTINCT shipment_id) as unique_shipments
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
        """Get items requiring human review by status"""
        query = """
        SELECT 
            CASE 
                WHEN quantity_check_result = 'FAIL' THEN 'Quantity Review'
                WHEN delivery_match = 'MISMATCH' THEN 'Delivery Review'
                WHEN match_confidence < 0.8 THEN 'Low Confidence Review'
                ELSE 'General Review'
            END as review_type,
            COUNT(*) as item_count,
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
        ORDER BY item_count DESC
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
            COUNT(DISTINCT s.shipment_id) as total_shipments,
            COUNT(DISTINCT emr.shipment_id) as matched_shipments,
            COUNT(DISTINCT CASE WHEN emr.quantity_check_result = 'FAIL' THEN emr.shipment_id END) as qty_issues,
            COUNT(DISTINCT CASE WHEN emr.delivery_match = 'MISMATCH' THEN emr.shipment_id END) as delivery_issues,
            CAST(COUNT(DISTINCT emr.shipment_id) * 100.0 / NULLIF(COUNT(DISTINCT s.shipment_id), 0) AS DECIMAL(5,1)) as match_rate_pct
        FROM customers c
        LEFT JOIN stg_fm_orders_shipped_table s ON s.customer_name LIKE c.canonical_name + '%'
        LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
        GROUP BY c.canonical_name, c.status
        HAVING COUNT(DISTINCT s.shipment_id) > 0
        ORDER BY total_shipments DESC
        """
        return self.execute_query(query)
    
    def get_recent_activity(self, days=7):
        """Get recent matching activity"""
        query = """
        SELECT 
            CAST(created_at AS DATE) as activity_date,
            COUNT(*) as matches_created,
            COUNT(DISTINCT customer_name) as customers_processed,
            COUNT(DISTINCT matching_session_id) as sessions_run
        FROM enhanced_matching_results
        WHERE created_at > DATEADD(day, -?, GETDATE())
        GROUP BY CAST(created_at AS DATE)
        ORDER BY activity_date DESC
        """
        return self.execute_query(query, [days])
    
    # === ORIGINAL METHODS (for compatibility) ===
    
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

def main():
    st.set_page_config(
        page_title="Enhanced Order Matching Dashboard",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("🎯 Enhanced Order Matching Dashboard")
    st.markdown("---")
    
    # Initialize configuration manager
    config_mgr = EnhancedConfigurationManager()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Select Page", [
        "🏠 Enhanced Dashboard",
        "🔍 HITL Review",
        "👥 Customer Management", 
        "🔗 Column Mappings",
        "🎯 Matching Strategies",
        "🚫 Exclusion Rules",
        "💎 Value Mappings",
        "📊 Audit Trail",
        "📤 Import/Export"
    ])
    
    if page == "🏠 Enhanced Dashboard":
        show_enhanced_dashboard(config_mgr)
    elif page == "🔍 HITL Review":
        show_hitl_review(config_mgr)
    elif page == "👥 Customer Management":
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

def show_enhanced_dashboard(config_mgr):
    """Enhanced dashboard with comprehensive analytics"""
    st.header("📊 System Overview & Analytics")
    
    try:
        # === TOP-LEVEL METRICS ===
        st.subheader("🎯 Key Performance Indicators")
        
        col1, col2, col3, col4 = st.columns(4)
        
        # Customer metrics
        customer_summary = config_mgr.get_customer_summary()
        total_customers = customer_summary['customer_count'].sum()
        
        with col1:
            st.metric("📊 Total Customers", total_customers)
            active_customers = customer_summary[customer_summary['status'] == 'approved']['customer_count'].sum() if not customer_summary.empty else 0
            st.caption(f"✅ {active_customers} Active")
        
        # Shipment metrics
        shipment_summary = config_mgr.get_shipment_summary()
        
        with col2:
            if not shipment_summary.empty:
                total_shipments = shipment_summary.iloc[0]['total_shipments']
                st.metric("📦 Total Shipments", total_shipments)
                matched_shipments = shipment_summary.iloc[0]['matched_shipments']
                st.caption(f"✅ {matched_shipments} Matched")
            else:
                st.metric("📦 Total Shipments", 0)
        
        # Review queue metrics
        review_summary = config_mgr.get_review_queue_summary()
        total_review_items = review_summary['item_count'].sum() if not review_summary.empty else 0
        
        with col3:
            st.metric("🔍 Items for Review", total_review_items)
            if not review_summary.empty:
                priority_items = review_summary[review_summary['review_type'] == 'Quantity Review']['item_count'].sum()
                st.caption(f"🚨 {priority_items} High Priority")
            else:
                st.caption("🎉 No items for review")
        
        # System health
        health_metrics = config_mgr.get_system_health_metrics()
        
        with col4:
            if not health_metrics.empty:
                avg_confidence = health_metrics.iloc[0]['avg_confidence']
                st.metric("🎯 Avg Confidence", f"{avg_confidence:.1%}" if avg_confidence else "N/A")
                recent_matches = health_metrics.iloc[0]['recent_matches']
                st.caption(f"📈 {recent_matches} Last 7 Days")
            else:
                st.metric("🎯 Avg Confidence", "N/A")
        
        # === LAYER ANALYSIS ===
        st.subheader("🎯 Match Layer Distribution")
        
        layer_dist = config_mgr.get_layer_distribution()
        
        if not layer_dist.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            layer_metrics = {
                'LAYER_0': {'name': 'Perfect Matches', 'icon': '🎯', 'color': '#28a745'},
                'LAYER_1': {'name': 'Fuzzy-Good', 'icon': '🔄', 'color': '#17a2b8'},
                'LAYER_2': {'name': 'Fuzzy-Deep', 'icon': '🔍', 'color': '#ffc107'},
                'LAYER_3': {'name': 'Quantity Resolution', 'icon': '🔧', 'color': '#6f42c1'}
            }
            
            columns = [col1, col2, col3, col4]
            
            for i, (layer, info) in enumerate(layer_metrics.items()):
                layer_data = layer_dist[layer_dist['match_layer'] == layer]
                count = layer_data['unique_shipments'].iloc[0] if not layer_data.empty else 0
                
                with columns[i]:
                    st.metric(f"{info['icon']} {info['name']}", count)
            
            # Layer distribution chart
            st.markdown("### 📈 Layer Distribution Visualization")
            
            chart_data = layer_dist.set_index('match_layer')['unique_shipments']
            st.bar_chart(chart_data)
        
        else:
            st.info("No layer distribution data available")
        
        # === REVIEW QUEUE BREAKDOWN ===
        st.subheader("🔍 Review Queue Analysis")
        
        if not review_summary.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📋 Items by Review Type**")
                st.dataframe(
                    review_summary[['review_type', 'item_count', 'affected_customers']],
                    use_container_width=True,
                    hide_index=True
                )
            
            with col2:
                st.markdown("**📊 Review Priority Distribution**")
                chart_data = review_summary.set_index('review_type')['item_count']
                st.bar_chart(chart_data)
        
        else:
            st.success("🎉 No items in review queue!")
        
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
        else:
            st.info("No customer activity data available")
        
        # === RECENT ACTIVITY ===
        st.subheader("📈 Recent System Activity")
        
        recent_activity = config_mgr.get_recent_activity(7)
        
        if not recent_activity.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📅 Daily Activity (Last 7 Days)**")
                st.line_chart(recent_activity.set_index('activity_date')['matches_created'])
            
            with col2:
                st.markdown("**📋 Activity Summary**")
                st.dataframe(
                    recent_activity[['activity_date', 'matches_created', 'customers_processed', 'sessions_run']],
                    use_container_width=True,
                    hide_index=True
                )
        else:
            st.info("No recent activity data available")
        
        # === SYSTEM STATUS ===
        st.subheader("🚀 System Status")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**⚡ Performance**")
            if not health_metrics.empty:
                total_sessions = health_metrics.iloc[0]['total_sessions']
                st.metric("Total Sessions", total_sessions)
                
                last_activity = health_metrics.iloc[0]['last_activity']
                if last_activity:
                    # Handle both datetime and string types
                    if isinstance(last_activity, str):
                        from datetime import datetime
                        try:
                            last_activity = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
                        except:
                            # Try pandas to_datetime as fallback
                            import pandas as pd
                            last_activity = pd.to_datetime(last_activity)
                    
                    hours_ago = (datetime.now() - last_activity).total_seconds() / 3600
                    st.caption(f"Last activity: {hours_ago:.1f} hours ago")
            
        with col2:
            st.markdown("**🎯 Quality Metrics**")
            if not health_metrics.empty:
                qty_failures = health_metrics.iloc[0]['quantity_failures']
                delivery_mismatches = health_metrics.iloc[0]['delivery_mismatches']
                
                st.metric("Quantity Failures", qty_failures)
                st.metric("Delivery Mismatches", delivery_mismatches)
        
        with col3:
            st.markdown("**🔧 Quick Actions**")
            
            if st.button("🔄 Refresh Data"):
                st.experimental_rerun()
            
            if st.button("📊 Run Diagnostics"):
                st.info("Diagnostic tools coming soon!")
            
            if st.button("🚀 Launch Matching"):
                st.info("Matching pipeline integration coming soon!")
        
    except Exception as e:
        st.error(f"Error loading enhanced dashboard: {str(e)}")
        st.exception(e)
        st.info("Make sure the database schema has been created and is accessible.")

# Placeholder functions for other pages
def show_hitl_review(config_mgr):
    st.header("🔍 HITL Review")
    st.info("HITL Review functionality - integrating from original app")

def show_customer_management(config_mgr):
    st.header("👥 Customer Management")
    st.info("Customer management functionality - integrating from original app")

def show_column_mappings(config_mgr):
    st.header("🔗 Column Mappings")
    st.info("Column mappings functionality - integrating from original app")

def show_matching_strategies(config_mgr):
    st.header("🎯 Matching Strategies")
    st.info("Matching strategies functionality - integrating from original app")

def show_exclusion_rules(config_mgr):
    st.header("🚫 Exclusion Rules")
    st.info("Exclusion rules functionality - integrating from original app")

def show_value_mappings(config_mgr):
    st.header("💎 Value Mappings")
    st.info("Value mappings functionality - integrating from original app")

def show_audit_trail(config_mgr):
    st.header("📊 Audit Trail")
    st.info("Audit trail functionality - integrating from original app")

def show_import_export(config_mgr):
    st.header("📤 Import/Export")
    st.info("Import/Export functionality - integrating from original app")

if __name__ == "__main__":
    main()
