import pyodbc
from auth_helper import get_connection_string

conn = pyodbc.connect(get_connection_string())
cursor = conn.cursor()

print("Current delivery/shipping column mappings:")
cursor.execute("""
    SELECT order_column, shipment_column 
    FROM column_mappings 
    WHERE order_column LIKE '%DELIVERY%' 
       OR shipment_column LIKE '%DELIVERY%' 
       OR shipment_column LIKE '%SHIPPING%'
""")

results = cursor.fetchall()
for row in results:
    print(f"  {row[0]} -> {row[1]}")

print("\nCurrent global matching strategy:")
cursor.execute("SELECT primary_match_fields FROM matching_strategies WHERE customer_id IS NULL")
result = cursor.fetchone()
if result:
    print(f"  {result[0]}")

conn.close()
