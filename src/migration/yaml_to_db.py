"""
YAML to Database Migration Script
Migrate canonical_customers.yaml configuration to database tables
"""

import yaml
import pyodbc
import json
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from auth_helper import get_connection_string

class YAMLMigrator:
    def __init__(self, yaml_file_path, connection_string=None):
        self.yaml_file_path = Path(yaml_file_path)
        self.connection_string = connection_string or get_connection_string()
        
    def load_yaml_config(self):
        """Load the YAML configuration file"""
        with open(self.yaml_file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def migrate_global_config(self, global_config):
        """Migrate global configuration to database"""
        print("Migrating global configuration...")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Clear existing global configs
            cursor.execute("DELETE FROM column_mappings WHERE customer_id IS NULL")
            cursor.execute("DELETE FROM matching_strategies WHERE customer_id IS NULL")
            cursor.execute("DELETE FROM exclusion_rules WHERE customer_id IS NULL")
            cursor.execute("DELETE FROM data_quality_keys WHERE customer_id IS NULL")
            
            # Migrate global column mappings
            if 'map' in global_config:
                for order_col, shipment_col in global_config['map'].items():
                    cursor.execute("""
                        INSERT INTO column_mappings (customer_id, order_column, shipment_column, priority, created_by, updated_by)
                        VALUES (NULL, ?, ?, 1, 'yaml_migrator', 'yaml_migrator')
                    """, order_col, shipment_col)
            
            # Migrate global matching strategy
            cursor.execute("""
                INSERT INTO matching_strategies (
                    customer_id, strategy_name, primary_match_fields, secondary_match_fields,
                    fuzzy_threshold, quantity_tolerance, confidence_high, confidence_medium, confidence_low,
                    created_by, updated_by
                ) VALUES (
                    NULL, 'Global Default Strategy', ?, ?, 0.85, 0.05, 0.90, 0.70, 0.50,
                    'yaml_migrator', 'yaml_migrator'
                )
            """, 
                json.dumps(["Style", "Color", "Customer_PO"]),
                json.dumps(["Shipping_Method", "Size"])
            )
            
            # Migrate global data quality keys
            if 'order_key_config' in global_config:
                order_config = global_config['order_key_config']
                if 'unique_keys' in order_config:
                    cursor.execute("""
                        INSERT INTO data_quality_keys (customer_id, table_name, key_type, field_names, description, created_by, updated_by)
                        VALUES (NULL, 'orders', 'unique_keys', ?, 'Global order unique keys', 'yaml_migrator', 'yaml_migrator')
                    """, json.dumps(order_config['unique_keys']))
                
                if 'extra_checks' in order_config:
                    cursor.execute("""
                        INSERT INTO data_quality_keys (customer_id, table_name, key_type, field_names, description, created_by, updated_by)
                        VALUES (NULL, 'orders', 'extra_checks', ?, 'Global order validation fields', 'yaml_migrator', 'yaml_migrator')
                    """, json.dumps(order_config['extra_checks']))
            
            if 'shipment_key_config' in global_config:
                shipment_config = global_config['shipment_key_config']
                if 'unique_keys' in shipment_config:
                    cursor.execute("""
                        INSERT INTO data_quality_keys (customer_id, table_name, key_type, field_names, description, created_by, updated_by)
                        VALUES (NULL, 'shipments', 'unique_keys', ?, 'Global shipment unique keys', 'yaml_migrator', 'yaml_migrator')
                    """, json.dumps(shipment_config['unique_keys']))
                
                if 'extra_checks' in shipment_config:
                    cursor.execute("""
                        INSERT INTO data_quality_keys (customer_id, table_name, key_type, field_names, description, created_by, updated_by)
                        VALUES (NULL, 'shipments', 'extra_checks', ?, 'Global shipment validation fields', 'yaml_migrator', 'yaml_migrator')
                    """, json.dumps(shipment_config['extra_checks']))
            
            # Add global exclusion rule for cancelled orders
            cursor.execute("""
                INSERT INTO exclusion_rules (
                    customer_id, table_name, field_name, exclude_values, rule_type, description, created_by, updated_by
                ) VALUES (
                    NULL, 'orders', 'order_type', '["CANCELLED"]', 'exclude', 
                    'Exclude cancelled orders from matching process', 'yaml_migrator', 'yaml_migrator'
                )
            """)
            
            conn.commit()
            print("✅ Global configuration migrated successfully")
    
    def migrate_customer(self, customer_config):
        """Migrate a single customer configuration"""
        canonical_name = customer_config['canonical']
        print(f"Migrating customer: {canonical_name}")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Insert customer record
            cursor.execute("""
                INSERT INTO customers (canonical_name, status, packed_products, shipped, master_order_list, created_by, updated_by)
                OUTPUT INSERTED.id
                VALUES (?, ?, ?, ?, ?, 'yaml_migrator', 'yaml_migrator')
            """, 
                canonical_name,
                customer_config.get('status', 'review'),
                customer_config.get('packed_products', ''),
                customer_config.get('shipped', ''),
                customer_config.get('master_order_list', '')
            )
            
            customer_id = cursor.fetchone()[0]
            
            # Insert aliases
            if 'aliases' in customer_config:
                for i, alias in enumerate(customer_config['aliases']):
                    cursor.execute("""
                        INSERT INTO customer_aliases (customer_id, alias_name, is_primary)
                        VALUES (?, ?, ?)
                    """, customer_id, alias, 1 if i == 0 else 0)
            
            # Insert customer-specific column mappings
            if 'map' in customer_config:
                for order_col, shipment_col in customer_config['map'].items():
                    cursor.execute("""
                        INSERT INTO column_mappings (customer_id, order_column, shipment_column, priority, created_by, updated_by)
                        VALUES (?, ?, ?, 1, 'yaml_migrator', 'yaml_migrator')
                    """, customer_id, order_col, shipment_col)
            
            # Insert customer-specific data quality keys
            if 'order_key_config' in customer_config:
                order_config = customer_config['order_key_config']
                if 'unique_keys' in order_config:
                    cursor.execute("""
                        INSERT INTO data_quality_keys (customer_id, table_name, key_type, field_names, description, created_by, updated_by)
                        VALUES (?, 'orders', 'unique_keys', ?, ?, 'yaml_migrator', 'yaml_migrator')
                    """, customer_id, json.dumps(order_config['unique_keys']), f'{canonical_name} order unique keys')
                
                if 'extra_checks' in order_config:
                    cursor.execute("""
                        INSERT INTO data_quality_keys (customer_id, table_name, key_type, field_names, description, created_by, updated_by)
                        VALUES (?, 'orders', 'extra_checks', ?, ?, 'yaml_migrator', 'yaml_migrator')
                    """, customer_id, json.dumps(order_config['extra_checks']), f'{canonical_name} order validation fields')
            
            if 'shipment_key_config' in customer_config:
                shipment_config = customer_config['shipment_key_config']
                if 'unique_keys' in shipment_config:
                    cursor.execute("""
                        INSERT INTO data_quality_keys (customer_id, table_name, key_type, field_names, description, created_by, updated_by)
                        VALUES (?, 'shipments', 'unique_keys', ?, ?, 'yaml_migrator', 'yaml_migrator')
                    """, customer_id, json.dumps(shipment_config['unique_keys']), f'{canonical_name} shipment unique keys')
                
                if 'extra_checks' in shipment_config:
                    cursor.execute("""
                        INSERT INTO data_quality_keys (customer_id, table_name, key_type, field_names, description, created_by, updated_by)
                        VALUES (?, 'shipments', 'extra_checks', ?, ?, 'yaml_migrator', 'yaml_migrator')
                    """, customer_id, json.dumps(shipment_config['extra_checks']), f'{canonical_name} shipment validation fields')
            
            # Handle special customer configurations
            if 'matching_config' in customer_config:
                matching_config = customer_config['matching_config']
                
                # Create customer-specific matching strategy
                primary_fields = ["Style", "Color", "Customer_PO"]  # Default
                secondary_fields = ["Shipping_Method", "Size"]  # Default
                fuzzy_threshold = matching_config.get('fuzzy_threshold', 85) / 100.0  # Convert percentage
                
                # Handle special cases like RHYTHM's alias_related_item strategy
                if matching_config.get('style_match_strategy') == 'alias_related_item':
                    primary_fields = ["ALIAS/RELATED ITEM", "Color", "Customer_PO"]
                
                cursor.execute("""
                    INSERT INTO matching_strategies (
                        customer_id, strategy_name, primary_match_fields, secondary_match_fields,
                        fuzzy_threshold, quantity_tolerance, confidence_high, confidence_medium, confidence_low,
                        created_by, updated_by
                    ) VALUES (
                        ?, ?, ?, ?, ?, 0.05, 0.90, 0.70, 0.50, 'yaml_migrator', 'yaml_migrator'
                    )
                """, 
                    customer_id,
                    f'{canonical_name} Custom Strategy',
                    json.dumps(primary_fields),
                    json.dumps(secondary_fields),
                    fuzzy_threshold
                )
                
                # Handle size aliases as value mappings
                if 'size_aliases' in matching_config:
                    for canonical_size, aliases in matching_config['size_aliases'].items():
                        for alias in aliases:
                            cursor.execute("""
                                INSERT INTO value_mappings (customer_id, field_name, source_value, canonical_value, mapping_type, created_by, updated_by)
                                VALUES (?, 'Size', ?, ?, 'exact', 'yaml_migrator', 'yaml_migrator')
                            """, customer_id, alias, canonical_size)
            
            conn.commit()
            print(f"✅ Customer {canonical_name} migrated successfully")
            return customer_id
    
    def run_migration(self):
        """Run the complete migration process"""
        print("Starting YAML to Database migration...")
        print(f"Source file: {self.yaml_file_path}")
        
        try:
            # Load YAML configuration
            config = self.load_yaml_config()
            
            # Migrate global configuration
            if 'global_config' in config:
                self.migrate_global_config(config['global_config'])
            
            # Migrate customers
            customer_count = 0
            if 'customers' in config:
                for customer_config in config['customers']:
                    try:
                        self.migrate_customer(customer_config)
                        customer_count += 1
                    except Exception as e:
                        print(f"❌ Error migrating customer {customer_config.get('canonical', 'unknown')}: {str(e)}")
                        continue
            
            print(f"\n🎉 Migration completed successfully!")
            print(f"   - Global configuration: ✅")
            print(f"   - Customers migrated: {customer_count}")
            
        except Exception as e:
            print(f"❌ Migration failed: {str(e)}")
            raise

def main():
    """Main migration execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate YAML configuration to database")
    parser.add_argument("--yaml-file", 
                       default="config/canonical_customers.yaml",
                       help="Path to YAML configuration file")
    parser.add_argument("--connection-string", 
                       help="Database connection string (optional)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be migrated without making changes")
    
    args = parser.parse_args()
    
    # Resolve yaml file path relative to project root
    project_root = Path(__file__).parent.parent.parent
    yaml_file = project_root / args.yaml_file
    
    if not yaml_file.exists():
        print(f"❌ YAML file not found: {yaml_file}")
        return 1
    
    try:
        migrator = YAMLMigrator(yaml_file, args.connection_string)
        
        if args.dry_run:
            print("🔍 DRY RUN MODE - No changes will be made")
            config = migrator.load_yaml_config()
            print(f"Would migrate:")
            print(f"  - Global config: {'✅' if 'global_config' in config else '❌'}")
            print(f"  - Customers: {len(config.get('customers', []))}")
            return 0
        
        migrator.run_migration()
        return 0
        
    except Exception as e:
        print(f"❌ Migration failed: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
