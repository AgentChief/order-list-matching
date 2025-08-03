"""
Authentication and Connection Helper
Provides database connection functionality
"""

import yaml
import logging

def load_config(path="config/config.yaml"):
    """Load configuration from YAML file"""
    try:
        with open(path, "r") as f:
            config = yaml.safe_load(f)
            return config
    except FileNotFoundError:
        logging.warning(f"Config file {path} not found, using defaults")
        return {}

def get_connection_string(db_key="orders"):
    """Get connection string from config file"""
    config = load_config()
    db_config = config.get('databases', {}).get(db_key, {})
    
    if 'conn_str' in db_config:
        return db_config['conn_str']
    
    # Fall back to default connection string
    return "Driver={SQL Server};Server=ross-db-srv-test.database.windows.net;Database=ORDERS;UID=admin_ross;PWD=Active@IT2023;Encrypt=yes;TrustServerCertificate=yes;"
