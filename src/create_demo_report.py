# Let's create a test report with artificial fuzzy and unmatched data to show the full functionality
from core import reporter
import pandas as pd
from datetime import datetime

# Simulate realistic data for demonstration
exact_matches = pd.DataFrame({
    'Customer_PO': ['4755', '4755', '4755'],
    'Shipping_Method': ['SEA', 'AIR', 'SEA-FB'],
    'Style': ['LFA24B05', 'LFA24K77', 'SYFA25B08'],
    'Color': ['476 - WOLF BLUE', '100 - ARCTIC', '417 - MALTESE BLUE'],
    'Shipped_Date': ['2025-06-05', '2025-06-14', '2025-06-05'],
    'Qty': [353, 213, 212],
    'method': ['exact', 'exact', 'exact'],
    'confidence': [1.0, 1.0, 1.0]
})

fuzzy_matches = pd.DataFrame({
    'Customer_PO': ['4755', '4755'],
    'Shipping_Method': ['AIR', 'SEA'],
    'Style': ['LFA25B68-VARIANT', 'SYFA25B11-ALT'],
    'Color': ['021 - DARK GREY', '417 - BLUE MALTESE'],
    'Shipped_Date': ['2025-06-27', '2025-06-05'],
    'Qty': [153, 418],
    'method': ['fuzzy', 'fuzzy'],
    'confidence': [0.85, 0.92]
})

unmatched = pd.DataFrame({
    'Customer_PO': ['4755', '4755', '4756'],
    'Shipping_Method': ['DHL', 'AIR', 'SEA'],
    'Style': ['UNKNOWN_STYLE', 'LFA24B05', 'LFA25B28'],
    'Color': ['999 - UNKNOWN', '476 - WOLF BLUE', '531 - WISTERIA'],
    'Shipped_Date': ['2025-06-05', '2025-07-01', '2025-07-12'],
    'Qty': [100, 50, 443]
})

orders = pd.DataFrame({
    'PO NUMBER': ['4755'] * 10,
    'PLANNED DELIVERY METHOD': ['SEA', 'AIR', 'SEA-FB'] * 3 + ['AIR'],
    'CUSTOMER STYLE': ['LFA24B05', 'LFA24K77', 'SYFA25B08'] * 3 + ['LFA25B68'],
    'CUSTOMER COLOUR DESCRIPTION': ['476 - WOLF BLUE', '100 - ARCTIC', '417 - MALTESE BLUE'] * 3 + ['021 - DARK GREY HEATHER']
})

results_data = {
    'exact_matches': exact_matches,
    'fuzzy_matches': fuzzy_matches,
    'unmatched': unmatched,
    'orders': orders,
    'join_cols': ['PO NUMBER', 'PLANNED DELIVERY METHOD', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION']
}

# Generate comprehensive demo report
reporter.print_summary('GREYSON', '4755', results_data)
report_file = reporter.generate_markdown_report('GREYSON', '4755_DEMO', results_data)
print(f'Demo report created: {report_file}')
