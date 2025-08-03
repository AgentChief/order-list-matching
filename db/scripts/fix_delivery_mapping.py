import pyodbc
from auth_helper import get_connection_string

conn = pyodbc.connect(get_connection_string())
cursor = conn.cursor()

# Update column mappings to use consistent field names
cursor.execute("""
    UPDATE column_mappings 
    SET shipment_column = 'Delivery_Method'
    WHERE order_column = 'PLANNED DELIVERY METHOD' 
    AND shipment_column = 'Shipping_Method'
""")

print(f"Updated {cursor.rowcount} column mappings")
conn.commit()
conn.close()
