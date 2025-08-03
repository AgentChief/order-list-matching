"""
Simple test script for the enhanced matching system
"""
import sys
from pathlib import Path

# Add project root to path  
sys.path.append(str(Path(__file__).parent.parent))

from auth_helper import get_connection_string
import pandas as pd
import pyodbc

def test_enhanced_system():
    """Test the enhanced matching system"""
    print("🎯 Testing Enhanced Layer-based Matching System")
    print("=" * 60)
    
    try:
        # Test database connection
        connection_string = get_connection_string()
        
        with pyodbc.connect(connection_string) as conn:
            print("✅ Database connection successful")
            
            # Test Layer 0, 1, 2 queries
            test_query = """
            SELECT 
                emr.match_layer,
                COUNT(*) as count,
                AVG(emr.match_confidence) as avg_confidence
            FROM enhanced_matching_results emr
            INNER JOIN stg_fm_orders_shipped_table s ON emr.shipment_id = s.shipment_id
            WHERE s.customer_name LIKE 'GREYSON%' AND s.po_number = '4755'
            GROUP BY emr.match_layer
            ORDER BY emr.match_layer
            """
            
            results = pd.read_sql(test_query, conn)
            
            if not results.empty:
                print("✅ Enhanced matching results found:")
                for _, row in results.iterrows():
                    layer = row['match_layer']
                    count = row['count']
                    confidence = row['avg_confidence'] or 0
                    
                    if layer == 'LAYER_0':
                        print(f"  🎯 Layer 0 (Exact): {count} matches")
                    elif layer == 'LAYER_1':
                        print(f"  🔄 Layer 1 (Fuzzy): {count} matches, {confidence:.1%} avg confidence")
                    elif layer == 'LAYER_2':
                        print(f"  🔍 Layer 2 (Deep): {count} matches, {confidence:.1%} avg confidence")
                
                total_matches = results['count'].sum()
                print(f"  📊 Total: {total_matches} matches")
                
                # Test unmatched count
                unmatched_query = """
                SELECT COUNT(*) as unmatched_count
                FROM stg_fm_orders_shipped_table s
                WHERE s.customer_name LIKE 'GREYSON%' AND s.po_number = '4755'
                    AND NOT EXISTS (
                        SELECT 1 FROM enhanced_matching_results emr 
                        WHERE emr.shipment_id = s.shipment_id
                    )
                """
                
                unmatched_result = pd.read_sql(unmatched_query, conn)
                unmatched_count = unmatched_result.iloc[0]['unmatched_count']
                
                print(f"  ❌ Unmatched: {unmatched_count} shipments")
                
                total_shipments = total_matches + unmatched_count
                match_rate = (total_matches / total_shipments * 100) if total_shipments > 0 else 0
                print(f"  🎯 Match Rate: {match_rate:.1f}%")
                
            else:
                print("⚠️ No enhanced matching results found")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        
    print("\n🚀 Ready to launch enhanced Streamlit interface!")
    print("Run: streamlit run src\\ui\\enhanced_streamlit_app.py --server.port 8502")

if __name__ == "__main__":
    test_enhanced_system()
