#!/usr/bin/env python3
"""
Mock API Server for Order Matching React Application Demo
Provides REST API endpoints with sample data (no database required)
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
import uuid

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000", "http://127.0.0.1:3000"])

# Mock data store
class MockDataStore:
    def __init__(self):
        self.orders = self._generate_mock_orders()
        self.shipments = self._generate_mock_shipments()
        self.matches = self._generate_mock_matches()
        self.movements = self._generate_mock_movements()
        
    def _generate_mock_orders(self):
        return [
            {'id': 1, 'customer_name': 'GREYSON', 'po_number': '4755', 'style_code': 'LSP24K59', 'color_description': 'NAVY BLUE', 'quantity': 100, 'created_at': '2025-03-01T10:00:00'},
            {'id': 2, 'customer_name': 'GREYSON', 'po_number': '4755', 'style_code': 'BSW24K10', 'color_description': 'WHITE', 'quantity': 50, 'created_at': '2025-03-01T11:00:00'},
            {'id': 3, 'customer_name': 'JOHNNIE_O', 'po_number': '5123', 'style_code': 'POL24S45', 'color_description': 'FOREST GREEN', 'quantity': 75, 'created_at': '2025-03-01T12:00:00'},
        ]
    
    def _generate_mock_shipments(self):
        return [
            {'shipment_id': 1001, 'customer_name': 'GREYSON', 'po_number': '4755', 'style_code': 'LSP24K59', 'color_description': 'NAVY BLUE', 'quantity': 95, 'shipped_date': '2025-03-02T14:00:00'},
            {'shipment_id': 1002, 'customer_name': 'GREYSON', 'po_number': '4755', 'style_code': 'BSW24K10', 'color_description': 'WHITE', 'quantity': 50, 'shipped_date': '2025-03-02T15:00:00'},
            {'shipment_id': 1003, 'customer_name': 'JOHNNIE_O', 'po_number': '5123', 'style_code': 'POL24S45', 'color_description': 'FOREST GREEN', 'quantity': 80, 'shipped_date': '2025-03-02T16:00:00'},
        ]
    
    def _generate_mock_matches(self):
        return [
            {'id': 1, 'customer_name': 'GREYSON', 'po_number': '4755', 'shipment_id': 1001, 'order_id': 1, 'match_layer': 'LAYER_0', 'match_confidence': 1.0, 'style_match': 'EXACT', 'color_match': 'EXACT', 'delivery_match': 'EXACT', 'quantity_check_result': 'FAIL', 'quantity_difference_percent': -5.0, 'created_at': '2025-03-02T16:00:00'},
            {'id': 2, 'customer_name': 'GREYSON', 'po_number': '4755', 'shipment_id': 1002, 'order_id': 2, 'match_layer': 'LAYER_0', 'match_confidence': 1.0, 'style_match': 'EXACT', 'color_match': 'EXACT', 'delivery_match': 'EXACT', 'quantity_check_result': 'PASS', 'quantity_difference_percent': 0.0, 'created_at': '2025-03-02T16:05:00'},
            {'id': 3, 'customer_name': 'JOHNNIE_O', 'po_number': '5123', 'shipment_id': 1003, 'order_id': 3, 'match_layer': 'LAYER_1', 'match_confidence': 0.92, 'style_match': 'EXACT', 'color_match': 'EXACT', 'delivery_match': 'SIMILAR', 'quantity_check_result': 'FAIL', 'quantity_difference_percent': 6.7, 'created_at': '2025-03-02T16:10:00'},
        ]
    
    def _generate_mock_movements(self):
        return [
            {'id': 1, 'order_id': '1', 'movement_type': 'ORDER_PLACED', 'movement_date': '2025-03-01T10:00:00', 'customer_name': 'GREYSON', 'po_number': '4755', 'style_code': 'LSP24K59', 'order_quantity': 100},
            {'id': 2, 'shipment_id': 1001, 'movement_type': 'SHIPMENT_SHIPPED', 'movement_date': '2025-03-02T14:00:00', 'customer_name': 'GREYSON', 'po_number': '4755', 'style_code': 'LSP24K59', 'shipped_quantity': 95},
            {'id': 3, 'movement_type': 'RECONCILED', 'movement_date': '2025-03-02T16:00:00', 'customer_name': 'GREYSON', 'po_number': '4755', 'reconciliation_status': 'MATCHED', 'reconciliation_confidence': 1.0},
        ]

mock_data = MockDataStore()

# Health check endpoint
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'database': 'mock_connected',
        'message': 'Mock API server running'
    })

# System status endpoint
@app.route('/api/system/status', methods=['GET'])
def get_system_status():
    """Get system status and metrics"""
    return jsonify({
        'connected': True,
        'last_update': datetime.now().isoformat(),
        'total_movements': len(mock_data.movements),
        'total_matches': len(mock_data.matches),
        'active_customers': 2,
        'hitl_queue_size': 2
    })

# Dashboard endpoints
@app.route('/api/dashboard/overview', methods=['GET'])
def get_dashboard_overview():
    """Get dashboard overview data"""
    return jsonify({
        'totalMovements': len(mock_data.movements),
        'totalMatches': len(mock_data.matches),
        'matchRate': 95.2,
        'hitlQueueSize': 2,
        'recentActivity': [
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
        ],
        'layerPerformance': [
            {'layer': 'LAYER_0', 'matches': 1250, 'confidence': 1.0},
            {'layer': 'LAYER_1', 'matches': 890, 'confidence': 0.92},
            {'layer': 'LAYER_2', 'matches': 456, 'confidence': 0.78},
            {'layer': 'LAYER_3', 'matches': 123, 'confidence': 0.65}
        ],
        'topCustomers': [
            {'name': 'GREYSON', 'value': 1450},
            {'name': 'JOHNNIE_O', 'value': 980},
            {'name': 'CUSTOMER_C', 'value': 756}
        ]
    })

# Data viewer endpoints
@app.route('/api/data/<table_name>', methods=['GET'])
def get_table_data(table_name):
    """Get paginated table data"""
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 100))
    
    if table_name == 'enhanced_matching_results':
        data = mock_data.matches
    elif table_name == 'fact_order_movements':
        data = mock_data.movements
    elif table_name == 'FACT_ORDER_LIST':
        data = mock_data.orders
    elif table_name == 'FM_orders_shipped':
        data = mock_data.shipments
    else:
        data = []
    
    # Simple pagination
    start = (page - 1) * limit
    end = start + limit
    paginated_data = data[start:end]
    
    return jsonify({
        'data': paginated_data,
        'total': len(data),
        'page': page,
        'limit': limit
    })

@app.route('/api/data/<table_name>/schema', methods=['GET'])
def get_table_schema(table_name):
    """Get table schema information"""
    schemas = {
        'enhanced_matching_results': [
            {'name': 'id', 'type': 'INT', 'nullable': 'NO'},
            {'name': 'customer_name', 'type': 'NVARCHAR', 'nullable': 'NO'},
            {'name': 'po_number', 'type': 'NVARCHAR', 'nullable': 'NO'},
            {'name': 'match_confidence', 'type': 'DECIMAL', 'nullable': 'YES'},
        ],
        'fact_order_movements': [
            {'name': 'id', 'type': 'BIGINT', 'nullable': 'NO'},
            {'name': 'movement_type', 'type': 'NVARCHAR', 'nullable': 'NO'},
            {'name': 'customer_name', 'type': 'NVARCHAR', 'nullable': 'NO'},
        ]
    }
    
    return jsonify(schemas.get(table_name, []))

# Queue management endpoints
@app.route('/api/queue/hitl', methods=['GET'])
def get_hitl_queue():
    """Get HITL review queue"""
    # Filter matches that need review
    hitl_items = [match for match in mock_data.matches if 
                  match['quantity_check_result'] == 'FAIL' or 
                  match['match_confidence'] < 0.8]
    
    # Add review reasons
    for item in hitl_items:
        if item['quantity_check_result'] == 'FAIL':
            item['review_reason'] = 'Quantity Review'
        elif item['match_confidence'] < 0.8:
            item['review_reason'] = 'Low Confidence'
        else:
            item['review_reason'] = 'General Review'
    
    return jsonify({
        'data': hitl_items,
        'total': len(hitl_items)
    })

@app.route('/api/queue/hitl/<int:match_id>/approve', methods=['POST'])
def approve_match(match_id):
    """Approve a match in HITL queue"""
    # Find and update the match
    for match in mock_data.matches:
        if match['id'] == match_id:
            match['quantity_check_result'] = 'PASS'
            match['updated_at'] = datetime.now().isoformat()
            break
    
    return jsonify({
        'success': True,
        'message': f'Match {match_id} approved successfully',
        'rows_affected': 1
    })

@app.route('/api/queue/hitl/<int:match_id>/reject', methods=['POST'])
def reject_match(match_id):
    """Reject a match in HITL queue"""
    # Find and update the match
    for match in mock_data.matches:
        if match['id'] == match_id:
            match['quantity_check_result'] = 'REJECTED'
            match['updated_at'] = datetime.now().isoformat()
            break
    
    return jsonify({
        'success': True,
        'message': f'Match {match_id} rejected successfully',
        'rows_affected': 1
    })

# Matching engine endpoints
@app.route('/api/matching/run', methods=['POST'])
def run_matching():
    """Run the enhanced matching engine"""
    data = request.get_json()
    customer_name = data.get('customer_name')
    po_number = data.get('po_number')
    
    if not customer_name:
        return jsonify({'error': 'Customer name is required'}), 400
    
    # Simulate matching results
    results = {
        'status': 'SUCCESS',
        'customer_name': customer_name,
        'po_number': po_number,
        'session_id': f'ENHANCED_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{str(uuid.uuid4())[:8]}',
        'total_orders': 12,
        'total_shipments': 15,
        'total_matches': 14,
        'unmatched_shipments': 1,
        'match_rate': 93.3,
        'layer_summary': {
            'LAYER_0': 8,
            'LAYER_1': 4,
            'LAYER_2': 2,
            'LAYER_3': 0
        },
        'matches': [
            {
                'shipment_id': 1001,
                'order_id': 1,
                'match_layer': 'LAYER_0',
                'confidence': 1.0,
                'style_code': 'LSP24K59',
                'color_description': 'NAVY BLUE',
                'shipment_quantity': 95,
                'order_quantity': 100,
                'quantity_variance': -5
            },
            {
                'shipment_id': 1002,
                'order_id': 2,
                'match_layer': 'LAYER_0',
                'confidence': 1.0,
                'style_code': 'BSW24K10',
                'color_description': 'WHITE',
                'shipment_quantity': 50,
                'order_quantity': 50,
                'quantity_variance': 0
            }
        ],
        'unmatched_shipment_ids': [1004]
    }
    
    return jsonify(results)

@app.route('/api/matching/history', methods=['GET'])
def get_matching_history():
    """Get matching execution history"""
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
        },
        {
            'session_id': f'ENHANCED_{(datetime.now() - timedelta(hours=2)).strftime("%Y%m%d_%H%M%S")}_XYZ789',
            'customer_name': 'JOHNNIE_O',
            'po_number': '5123',
            'total_shipments': 89,
            'total_matches': 87,
            'match_rate': 97.8,
            'status': 'SUCCESS',
            'created_at': (datetime.now() - timedelta(hours=2)).isoformat()
        }
    ]
    
    return jsonify({
        'data': history,
        'total': len(history)
    })

# Stored procedure endpoints
@app.route('/api/procedures/execute', methods=['POST'])
def execute_procedure():
    """Execute a stored procedure"""
    data = request.get_json()
    procedure_name = data.get('procedure_name')
    parameters = data.get('parameters', {})
    
    if not procedure_name:
        return jsonify({'error': 'Procedure name is required'}), 400
    
    # Simulate procedure execution
    result = {
        'success': True,
        'procedure_name': procedure_name,
        'parameters': parameters,
        'executed_at': datetime.now().isoformat(),
        'duration_ms': 1250,
        'rows_affected': 156,
        'result_data': f'Procedure {procedure_name} executed successfully with parameters: {parameters}'
    }
    
    return jsonify(result)

@app.route('/api/procedures/list', methods=['GET'])
def get_available_procedures():
    """Get list of available stored procedures"""
    procedures = [
        {'name': 'sp_capture_order_placed', 'description': 'Capture order placement event'},
        {'name': 'sp_capture_shipment_created', 'description': 'Capture shipment creation event'},
        {'name': 'sp_capture_shipment_shipped', 'description': 'Capture shipment shipped event'},
        {'name': 'sp_capture_reconciliation_event', 'description': 'Capture reconciliation event'},
        {'name': 'sp_populate_movement_table_from_existing', 'description': 'Populate movement table from existing data'},
        {'name': 'sp_update_cumulative_quantities', 'description': 'Update cumulative quantities'}
    ]
    
    return jsonify(procedures)

# Analytics endpoints
@app.route('/api/analytics/layer-performance', methods=['GET'])
def get_layer_performance():
    """Get layer performance analytics"""
    data = [
        {'layer': 'LAYER_0', 'matches': 1250, 'avgConfidence': 1.0, 'successRate': 100},
        {'layer': 'LAYER_1', 'matches': 890, 'avgConfidence': 0.92, 'successRate': 95},
        {'layer': 'LAYER_2', 'matches': 456, 'avgConfidence': 0.78, 'successRate': 87},
        {'layer': 'LAYER_3', 'matches': 123, 'avgConfidence': 0.65, 'successRate': 72}
    ]
    
    return jsonify(data)

@app.route('/api/analytics/customer-performance', methods=['GET'])
def get_customer_performance():
    """Get customer performance analytics"""
    data = [
        {'customer': 'GREYSON', 'totalMatches': 1450, 'avgConfidence': 0.89, 'issues': 23},
        {'customer': 'JOHNNIE_O', 'totalMatches': 980, 'avgConfidence': 0.91, 'issues': 12},
        {'customer': 'CUSTOMER_C', 'totalMatches': 756, 'avgConfidence': 0.85, 'issues': 34},
        {'customer': 'CUSTOMER_D', 'totalMatches': 543, 'avgConfidence': 0.88, 'issues': 18}
    ]
    
    return jsonify(data)

@app.route('/api/analytics/matching-trends', methods=['GET'])
def get_matching_trends():
    """Get matching trends over time"""
    days = int(request.args.get('days', 30))
    
    # Generate sample trend data
    data = []
    for i in range(min(days, 10)):  # Limit to 10 days for demo
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        data.append({
            'date': date,
            'matches': 140 + (i * 10) + (i % 3 * 15),
            'rate': 89.5 + (i % 5 * 1.2)
        })
    
    return jsonify(data)

@app.route('/api/analytics/system-performance', methods=['GET'])
def get_system_performance():
    """Get system performance metrics"""
    return jsonify({
        'avgQueryTime': 0.18,
        'cacheHitRate': 98.5,
        'matchingThroughput': 1245,
        'systemLoad': 68
    })

# Utility endpoints
@app.route('/api/customers', methods=['GET'])
def get_customers():
    """Get list of customers"""
    customers = ['GREYSON', 'JOHNNIE_O', 'CUSTOMER_C', 'CUSTOMER_D']
    return jsonify(customers)

@app.route('/api/schema/tables', methods=['GET'])
def get_available_tables():
    """Get list of available tables"""
    tables = [
        {'name': 'fact_order_movements', 'displayName': 'Movement Table', 'type': 'BASE TABLE'},
        {'name': 'enhanced_matching_results', 'displayName': 'Matching Results', 'type': 'BASE TABLE'},
        {'name': 'FACT_ORDER_LIST', 'displayName': 'Order List', 'type': 'BASE TABLE'},
        {'name': 'FM_orders_shipped', 'displayName': 'Shipped Orders', 'type': 'BASE TABLE'},
        {'name': 'shipment_summary_cache', 'displayName': 'Shipment Cache', 'type': 'BASE TABLE'}
    ]
    
    return jsonify(tables)

if __name__ == '__main__':
    print("🚀 Starting Mock API Server for Order Matching Demo...")
    print(f"📊 Server running on http://localhost:8001")
    print(f"🎯 CORS enabled for React frontend")
    print(f"🔧 Mock data available - no database required!")
    
    app.run(
        host='0.0.0.0',
        port=8001,
        debug=True,
        threaded=True
    )