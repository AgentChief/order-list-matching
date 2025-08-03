import pyodbc
from auth_helper import get_connection_string

conn = pyodbc.connect(get_connection_string())
cursor = conn.cursor()

# Update global matching strategy to include delivery method (using correct field name)
cursor.execute('''
    UPDATE matching_strategies 
    SET primary_match_fields = ? 
    WHERE customer_id IS NULL
''', '["Style", "Color", "Delivery_Method"]')

conn.commit()
print("Updated global matching strategy to include delivery method")
conn.close()
