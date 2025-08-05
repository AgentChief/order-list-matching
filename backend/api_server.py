#!/usr/bin/env python3
"""
Backend API Server for Order Matching React Application
Provides REST API endpoints for the React + Electron frontend
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import sys
import os
from pathlib import Path
import pyodbc
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
import traceback
import uuid

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from auth_helper import get_connection_string
except ImportError:
    print("Warning: auth_helper not found, using environment variables")
    def get_connection_string():
        return os.environ.get('DATABASE_CONNECTION_STRING', '')

# Import enhanced matching engine
try:
    sys.path.append(str(project_root / 'src' / 'reconciliation'))
    from enhanced_matching_engine import EnhancedMatchingEngine
except ImportError:
    print("Warning: Enhanced matching engine not available")
    EnhancedMatchingEngine = None

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000", "http://127.0.0.1:3000"])

class DatabaseManager:
    """Database connection and query management"""
    
    def __init__(self):
        self.connection_string = get_connection_string()
        
    def get_connection(self):
        """Get database connection"""
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise
    
    def execute_query(self, query, params=None, fetch=True):
        """Execute query and return results"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if fetch:
                    columns = [column[0] for column in cursor.description] if cursor.description else []
                    rows = cursor.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    conn.commit()
                    return cursor.rowcount
                    
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_procedure(self, procedure_name, params=None):
        """Execute stored procedure"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if params:
                    param_placeholders = ', '.join(['?' for _ in params])
                    query = f"EXEC {procedure_name} {param_placeholders}"
                    cursor.execute(query, params)
                else:
                    cursor.execute(f"EXEC {procedure_name}")
                
                # Try to fetch results
                try:
                    columns = [column[0] for column in cursor.description] if cursor.description else []
                    rows = cursor.fetchall()
                    result_data = [dict(zip(columns, row)) for row in rows]
                except:
                    result_data = None
                
                conn.commit()
                
                return {
                    'success': True,
                    'rows_affected': cursor.rowcount,
                    'result_data': result_data
                }
                
        except Exception as e:
            logger.error(f"Procedure execution failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'rows_affected': 0
            }

db = DatabaseManager()

# Error handler
@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"Unhandled exception: {str(e)}")
    logger.error(traceback.format_exc())
    return jsonify({
        'error': str(e),
        'message': 'An internal server error occurred'
    }), 500

# Health check endpoint
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected'
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'disconnected',
            'error': str(e)
        }), 500

# System status endpoint
@app.route('/api/system/status', methods=['GET'])
def get_system_status():
    """Get system status and metrics"""
    try:
        # Get basic counts
        queries = {
            'total_movements': "SELECT COUNT(*) as count FROM fact_order_movements",
            'total_matches': "SELECT COUNT(*) as count FROM enhanced_matching_results",
            'active_customers': "SELECT COUNT(DISTINCT customer_name) as count FROM enhanced_matching_results",
            'hitl_queue_size': """
                SELECT COUNT(*) as count FROM enhanced_matching_results 
                WHERE quantity_check_result = 'FAIL' OR delivery_match = 'MISMATCH' OR match_confidence < 0.8
            """
        }
        
        results = {}
        for key, query in queries.items():
            try:
                result = db.execute_query(query)
                results[key] = result[0]['count'] if result else 0
            except:
                results[key] = 0
        
        return jsonify({
            'connected': True,
            'last_update': datetime.now().isoformat(),
            **results
        })
        
    except Exception as e:
        return jsonify({
            'connected': False,
            'error': str(e),
            'total_movements': 0,
            'total_matches': 0,
            'active_customers': 0,
            'hitl_queue_size': 0
        })

# Dashboard endpoints
@app.route('/api/dashboard/overview', methods=['GET'])
def get_dashboard_overview():
    """Get dashboard overview data"""
    try:
        # System metrics
        system_metrics = db.execute_query("""
            SELECT 
                COUNT(*) as total_movements,
                COUNT(DISTINCT CASE WHEN movement_type = 'ORDER_PLACED' THEN order_id END) as total_orders,
                COUNT(DISTINCT CASE WHEN movement_type = 'SHIPMENT_SHIPPED' THEN shipment_id END) as total_shipments
            FROM fact_order_movements
        """)[0] if db.execute_query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'fact_order_movements'")[0][''] > 0 else {'total_movements': 0, 'total_orders': 0, 'total_shipments': 0}
        
        # Matching metrics
        try:
            matching_metrics = db.execute_query("""
                SELECT 
                    COUNT(*) as total_matches,
                    AVG(match_confidence) as avg_confidence,
                    COUNT(CASE WHEN quantity_check_result = 'FAIL' OR delivery_match = 'MISMATCH' OR match_confidence < 0.8 THEN 1 END) as hitl_queue_size
                FROM enhanced_matching_results
            """)[0]
        except:
            matching_metrics = {'total_matches': 0, 'avg_confidence': 0, 'hitl_queue_size': 0}
        
        # Layer performance
        try:
            layer_performance = db.execute_query("""
                SELECT 
                    match_layer as layer,
                    COUNT(*) as matches,
                    AVG(match_confidence) as confidence
                FROM enhanced_matching_results
                GROUP BY match_layer
                ORDER BY match_layer
            """)
        except:
            layer_performance = []
        
        # Top customers
        try:
            top_customers = db.execute_query("""
                SELECT TOP 5
                    customer_name as name,
                    COUNT(*) as value
                FROM enhanced_matching_results
                GROUP BY customer_name
                ORDER BY COUNT(*) DESC
            """)
        except:
            top_customers = []
        
        # Recent activity (mock data for now)
        recent_activity = [
            {
                'id': 1,
                'timestamp': (datetime.now() - timedelta(minutes=5)).isoformat(),
                'activity': 'Enhanced matching completed',
                'customer': 'GREYSON',
                'status': 'success'
            },
            {
                'id': 2,
                'timestamp': (datetime.now() - timedelta(minutes=15)).isoformat(),
                'activity': 'HITL review approved',
                'customer': 'JOHNNIE_O',
                'status': 'success'
            }
        ]
        
        # Calculate match rate
        match_rate = 0
        if system_metrics['total_shipments'] > 0:
            match_rate = (matching_metrics['total_matches'] / system_metrics['total_shipments']) * 100
        
        return jsonify({
            'totalMovements': system_metrics['total_movements'],
            'totalMatches': matching_metrics['total_matches'],
            'matchRate': match_rate,
            'hitlQueueSize': matching_metrics['hitl_queue_size'],
            'recentActivity': recent_activity,
            'layerPerformance': layer_performance,
            'topCustomers': top_customers
        })
        
    except Exception as e:
        logger.error(f"Dashboard overview error: {str(e)}")
        return jsonify({
            'totalMovements': 0,
            'totalMatches': 0,
            'matchRate': 0,
            'hitlQueueSize': 0,
            'recentActivity': [],
            'layerPerformance': [],
            'topCustomers': []
        })

# Data viewer endpoints
@app.route('/api/data/<table_name>', methods=['GET'])
def get_table_data(table_name):
    """Get paginated table data"""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 100))
        offset = (page - 1) * limit
        
        # Validate table name to prevent SQL injection
        allowed_tables = [
            'fact_order_movements', 'enhanced_matching_results', 
            'FACT_ORDER_LIST', 'FM_orders_shipped', 'shipment_summary_cache'
        ]
        
        if table_name not in allowed_tables:
            return jsonify({'error': 'Table not allowed'}), 400
        
        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM {table_name}"
        total_result = db.execute_query(count_query)
        total = total_result[0]['total'] if total_result else 0
        
        # Get data
        data_query = f"SELECT * FROM {table_name} ORDER BY 1 OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        data = db.execute_query(data_query)
        
        return jsonify({
            'data': data,
            'total': total,
            'page': page,
            'limit': limit
        })
        
    except Exception as e:
        logger.error(f"Get table data error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/data/<table_name>/schema', methods=['GET'])
def get_table_schema(table_name):
    """Get table schema information"""
    try:
        schema_query = """
            SELECT 
                COLUMN_NAME as name,
                DATA_TYPE as type,
                IS_NULLABLE as nullable,
                COLUMN_DEFAULT as defaultValue
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
        """
        
        schema = db.execute_query(schema_query, [table_name])
        return jsonify(schema)
        
    except Exception as e:
        logger.error(f"Get table schema error: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Queue management endpoints
@app.route('/api/queue/hitl', methods=['GET'])
def get_hitl_queue():
    """Get HITL review queue"""
    try:
        customer_filter = request.args.get('customer', '')
        
        query = """
            SELECT 
                id, customer_name, po_number, shipment_id, order_id,
                match_layer, match_confidence, style_match, color_match, delivery_match,
                quantity_check_result, quantity_difference_percent, created_at,
                CASE 
                    WHEN quantity_check_result = 'FAIL' THEN 'Quantity Review'
                    WHEN delivery_match = 'MISMATCH' THEN 'Delivery Review'
                    WHEN match_confidence < 0.8 THEN 'Low Confidence'
                    ELSE 'General Review'
                END as review_reason
            FROM enhanced_matching_results
            WHERE quantity_check_result = 'FAIL' 
               OR delivery_match = 'MISMATCH'
               OR match_confidence < 0.8
        """
        
        params = []
        if customer_filter:
            query += " AND customer_name LIKE ?"
            params.append(f"%{customer_filter}%")
        
        query += " ORDER BY match_confidence ASC, created_at DESC"
        
        data = db.execute_query(query, params)
        
        return jsonify({
            'data': data,
            'total': len(data)
        })
        
    except Exception as e:
        logger.error(f"Get HITL queue error: {str(e)}")
        return jsonify({'data': [], 'total': 0})

@app.route('/api/queue/hitl/<int:match_id>/approve', methods=['POST'])
def approve_match(match_id):
    """Approve a match in HITL queue"""
    try:
        data = request.get_json()
        justification = data.get('justification', '')
        
        # Update the match status
        update_query = """
            UPDATE enhanced_matching_results 
            SET quantity_check_result = 'PASS',
                updated_at = GETDATE(),
                review_notes = ?
            WHERE id = ?
        """
        
        rows_affected = db.execute_query(update_query, [justification, match_id], fetch=False)
        
        return jsonify({
            'success': True,
            'message': f'Match {match_id} approved successfully',
            'rows_affected': rows_affected
        })
        
    except Exception as e:
        logger.error(f"Approve match error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Matching engine endpoints
@app.route('/api/matching/run', methods=['POST'])
def run_matching():
    """Run the enhanced matching engine"""
    try:
        if not EnhancedMatchingEngine:
            return jsonify({'error': 'Matching engine not available'}), 500
        
        data = request.get_json()
        customer_name = data.get('customer_name')
        po_number = data.get('po_number')
        
        if not customer_name:
            return jsonify({'error': 'Customer name is required'}), 400
        
        # Run enhanced matching
        engine = EnhancedMatchingEngine()
        results = engine.run_enhanced_matching(customer_name, po_number)
        
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Run matching error: {str(e)}")
        return jsonify({
            'status': 'ERROR',
            'error': str(e),
            'total_matches': 0,
            'total_shipments': 0,
            'match_rate': 0
        }), 500

@app.route('/api/matching/history', methods=['GET'])
def get_matching_history():
    """Get matching execution history"""
    try:
        limit = int(request.args.get('limit', 50))
        
        # This would come from a matching_history table in real implementation
        history = [
            {
                'session_id': f'ENHANCED_{datetime.now().strftime("%Y%m%d_%H%M%S")}_ABC123',
                'customer_name': 'GREYSON',
                'po_number': '4755',
                'total_shipments': 145,
                'total_matches': 134,
                'match_rate': 92.4,
                'status': 'SUCCESS',
                'created_at': datetime.now().isoformat()
            }
        ]
        
        return jsonify({
            'data': history[:limit],
            'total': len(history)
        })
        
    except Exception as e:
        logger.error(f"Get matching history error: {str(e)}")
        return jsonify({'data': [], 'total': 0})

# Stored procedure endpoints
@app.route('/api/procedures/execute', methods=['POST'])
def execute_procedure():
    """Execute a stored procedure"""
    try:
        data = request.get_json()
        procedure_name = data.get('procedure_name')
        parameters = data.get('parameters', {})
        
        if not procedure_name:
            return jsonify({'error': 'Procedure name is required'}), 400
        
        # Convert parameters to list for execution
        param_values = list(parameters.values()) if parameters else None
        
        result = db.execute_procedure(procedure_name, param_values)
        
        return jsonify({
            **result,
            'procedure_name': procedure_name,
            'parameters': parameters,
            'executed_at': datetime.now().isoformat(),
            'duration_ms': 100  # Mock duration
        })
        
    except Exception as e:
        logger.error(f"Execute procedure error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'rows_affected': 0
        }), 500

@app.route('/api/procedures/list', methods=['GET'])
def get_available_procedures():
    """Get list of available stored procedures"""
    try:
        query = """
            SELECT 
                ROUTINE_NAME as name,
                ROUTINE_DEFINITION as definition
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'
                AND ROUTINE_NAME LIKE 'sp_%'
        """
        
        procedures = db.execute_query(query)
        
        # Add descriptions for known procedures
        descriptions = {
            'sp_capture_order_placed': 'Capture order placement event',
            'sp_capture_shipment_created': 'Capture shipment creation event',
            'sp_capture_shipment_shipped': 'Capture shipment shipped event',
            'sp_capture_reconciliation_event': 'Capture reconciliation event',
            'sp_populate_movement_table_from_existing': 'Populate movement table from existing data',
            'sp_update_cumulative_quantities': 'Update cumulative quantities'
        }
        
        for proc in procedures:
            proc['description'] = descriptions.get(proc['name'], 'No description available')
        
        return jsonify(procedures)
        
    except Exception as e:
        logger.error(f"Get procedures error: {str(e)}")
        return jsonify([])

# Analytics endpoints
@app.route('/api/analytics/layer-performance', methods=['GET'])
def get_layer_performance():
    """Get layer performance analytics"""
    try:
        query = """
            SELECT 
                match_layer as layer,
                COUNT(*) as matches,
                AVG(match_confidence) as avgConfidence,
                COUNT(CASE WHEN quantity_check_result = 'PASS' THEN 1 END) * 100.0 / COUNT(*) as successRate
            FROM enhanced_matching_results
            GROUP BY match_layer
            ORDER BY match_layer
        """
        
        data = db.execute_query(query)
        return jsonify(data)
        
    except Exception as e:
        logger.error(f"Get layer performance error: {str(e)}")
        return jsonify([])

@app.route('/api/analytics/customer-performance', methods=['GET'])
def get_customer_performance():
    """Get customer performance analytics"""
    try:
        query = """
            SELECT 
                customer_name as customer,
                COUNT(*) as totalMatches,
                AVG(match_confidence) as avgConfidence,
                COUNT(CASE WHEN quantity_check_result = 'FAIL' THEN 1 END) as issues
            FROM enhanced_matching_results
            GROUP BY customer_name
            ORDER BY COUNT(*) DESC
        """
        
        data = db.execute_query(query)
        return jsonify(data)
        
    except Exception as e:
        logger.error(f"Get customer performance error: {str(e)}")
        return jsonify([])

# Utility endpoints
@app.route('/api/customers', methods=['GET'])
def get_customers():
    """Get list of customers"""
    try:
        # Try multiple tables to get customers
        queries = [
            "SELECT DISTINCT customer_name as customer FROM enhanced_matching_results",
            "SELECT DISTINCT customer_name as customer FROM FACT_ORDER_LIST",
            "SELECT DISTINCT Customer as customer FROM FM_orders_shipped"
        ]
        
        customers = set()
        for query in queries:
            try:
                results = db.execute_query(query)
                customers.update([r['customer'] for r in results if r['customer']])
            except:
                continue
        
        return jsonify(sorted(list(customers)))
        
    except Exception as e:
        logger.error(f"Get customers error: {str(e)}")
        return jsonify(['GREYSON', 'JOHNNIE_O'])  # Fallback

@app.route('/api/schema/tables', methods=['GET'])
def get_available_tables():
    """Get list of available tables"""
    try:
        query = """
            SELECT 
                TABLE_NAME as name,
                TABLE_TYPE as type
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME IN ('fact_order_movements', 'enhanced_matching_results', 
                                  'FACT_ORDER_LIST', 'FM_orders_shipped', 'shipment_summary_cache')
        """
        
        tables = db.execute_query(query)
        
        # Add display names
        display_names = {
            'fact_order_movements': 'Movement Table',
            'enhanced_matching_results': 'Matching Results',
            'FACT_ORDER_LIST': 'Order List',
            'FM_orders_shipped': 'Shipped Orders',
            'shipment_summary_cache': 'Shipment Cache'
        }
        
        for table in tables:
            table['displayName'] = display_names.get(table['name'], table['name'])
        
        return jsonify(tables)
        
    except Exception as e:
        logger.error(f"Get tables error: {str(e)}")
        return jsonify([])

if __name__ == '__main__':
    print("🚀 Starting Order Matching API Server...")
    print(f"📊 Server will run on http://localhost:8001")
    print(f"🎯 CORS enabled for React frontend")
    
    app.run(
        host='0.0.0.0',
        port=8001,
        debug=True,
        threaded=True
    )