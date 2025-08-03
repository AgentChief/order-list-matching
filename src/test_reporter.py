from core import reporter
import pandas as pd

# Create test data
exact_matches = pd.DataFrame({'PO': ['4755'], 'Style': ['TEST'], 'method': ['exact']})
fuzzy_matches = pd.DataFrame()  
unmatched = pd.DataFrame()
orders = pd.DataFrame({'PO': ['4755']})

results_data = {
    'exact_matches': exact_matches,
    'fuzzy_matches': fuzzy_matches,
    'unmatched': unmatched,
    'orders': orders,
    'join_cols': ['PO', 'Style']
}

try:
    report_file = reporter.generate_markdown_report('GREYSON', '4755', results_data)
    print(f'Success: {report_file}')
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
