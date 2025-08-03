"""
get_greyson_po4755_data.py

- Connects to the ORDERS_UNIFIED and FM_orders_shipped tables
- Extracts all orders and shipments for GREYSON/GREYSON CLOTHIERS, PO 4755
- Aggregates shipments at the style/color/shipping_method/shipped_date level (no size)
- Outputs the first 20 rows and total count for each DataFrame
"""
import pandas as pd
import numpy as np
from utils.db_helper import run_query

# Helper function to normalize string values
def norm(x):
    return str(x).upper().strip() if pd.notnull(x) else ""

def main():
    print("Starting Greyson PO 4755 data analysis...")
    
    # Query orders for GREYSON, PO 4755
    orders_sql = """
    SELECT * FROM ORDERS_UNIFIED
    WHERE [CUSTOMER NAME] IN ('GREYSON', 'GREYSON CLOTHIERS')
      AND [PO NUMBER] = '4755'
    """
    print("Fetching orders data...")
    orders = run_query(orders_sql, db_key='orders')
    print(f"Orders loaded: {len(orders)} rows")

    # Query shipments for GREYSON, PO 4755
    shipments_sql = """
    SELECT * FROM FM_orders_shipped
    WHERE [Customer] IN ('GREYSON', 'GREYSON CLOTHIERS')
      AND [Customer_PO] = '4755'
    """
    print("Fetching shipments data...")
    shipments = run_query(shipments_sql, db_key='orders')
    print(f"Shipments loaded: {len(shipments)} rows")

    # Aggregate shipments at style/color/shipping_method/shipped_date
    group_cols = [
        'Customer_PO', 'Style', 'Color', 'Shipping_Method', 'Shipped_Date'
    ]
    present_cols = [c for c in group_cols if c in shipments.columns]
    if 'Qty' in shipments.columns:
        agg = shipments.groupby(present_cols, dropna=False, as_index=False)['Qty'].sum()
    else:
        agg = shipments.groupby(present_cols, dropna=False, as_index=False).size().rename(columns={'size': 'Count'})
    print(f"Aggregated shipments: {len(agg)} rows")

    # --- STYLE MATCHING ---
    order_styles = set()
    shipment_styles = set()
    if 'CUSTOMER STYLE' in orders.columns:
        order_styles = set(orders['CUSTOMER STYLE'].dropna().astype(str).str.upper())
    if 'Style' in agg.columns:
        shipment_styles = set(agg['Style'].dropna().astype(str).str.upper())
    matched_styles = shipment_styles & order_styles
    unmatched_styles = shipment_styles - order_styles
    print("\n--- STYLE MATCH SUMMARY (AGG SHIPMENTS vs ORDERS for PO 4755) ---")
    print(f"Total shipment styles: {len(shipment_styles)}")
    print(f"Total order styles: {len(order_styles)}")
    print(f"Matched styles: {len(matched_styles)}")
    print(f"Unmatched styles: {len(unmatched_styles)}")
    if len(unmatched_styles) == 0:
        print("All shipment styles for PO 4755 are present in the order list.")
    else:
        print(f"WARNING: {len(unmatched_styles)} shipment styles for PO 4755 are NOT present in the order list!")

    # --- STYLE + COLOR MATCHING (on PO) ---
    order_style_color = set()
    shipment_style_color = set()
    if 'CUSTOMER STYLE' in orders.columns and 'CUSTOMER COLOUR DESCRIPTION' in orders.columns:
        order_style_color = set(
            (norm(row['CUSTOMER STYLE']), norm(row['CUSTOMER COLOUR DESCRIPTION']))
            for _, row in orders.iterrows()
        )
    if 'Style' in agg.columns and 'Color' in agg.columns:
        shipment_style_color = set(
            (norm(row['Style']), norm(row['Color']))
            for _, row in agg.iterrows()
        )
    matched_style_color = shipment_style_color & order_style_color
    unmatched_style_color = shipment_style_color - order_style_color

    print("\n--- STYLE + COLOR MATCH SUMMARY (AGG SHIPMENTS vs ORDERS for PO 4755) ---")
    print(f"Total shipment style+color pairs: {len(shipment_style_color)}")
    print(f"Total order style+color pairs: {len(order_style_color)}")
    print(f"Matched style+color pairs: {len(matched_style_color)}")
    print(f"Unmatched style+color pairs: {len(unmatched_style_color)}")
    if len(unmatched_style_color) == 0:
        print("All shipment style+color pairs for PO 4755 are present in the order list.")
    else:
        print(f"WARNING: {len(unmatched_style_color)} shipment style+color pairs for PO 4755 are NOT present in the order list!")

        # Print the unmatched shipment (style, color) row
        print("\n--- UNMATCHED SHIPMENT STYLE+COLOR ROW(S) ---")
        for style, color in unmatched_style_color:
            row = agg.loc[
                (agg['Style'].astype(str).str.upper().str.strip() == style) &
                (agg['Color'].astype(str).str.upper().str.strip() == color)
            ]
            print(row.to_string(index=False))

        # Fuzzy match fallback for unmatched shipment style+color
        try:
            from rapidfuzz import process, fuzz
            
            print("\n--- FUZZY MATCH ATTEMPT FOR UNMATCHED SHIPMENT STYLE+COLOR ---")
            order_style_color_list = list(order_style_color)
            for style, color in unmatched_style_color:
                # Fuzzy match on style and color separately, then combine scores
                best_match = None
                best_score = 0
                for o_style, o_color in order_style_color_list:
                    style_score = fuzz.ratio(style, o_style)
                    color_score = fuzz.ratio(color, o_color)
                    avg_score = (style_score + color_score) / 2
                    if avg_score > best_score:
                        best_score = avg_score
                        best_match = (o_style, o_color)
                if best_score >= 90:
                    print(f"Fuzzy match found for shipment (Style: '{style}', Color: '{color}'):")
                    print(f"  → Closest order (Style: '{best_match[0]}', Color: '{best_match[1]}') with confidence {best_score:.1f}%")
                else:
                    print(f"No high-confidence fuzzy match found for shipment (Style: '{style}', Color: '{color}'). Best score: {best_score:.1f}%")
        except ImportError:
            print("rapidfuzz not installed, skipping fuzzy matching.")
    
    # Build output DataFrame for style+color match results
    match_results = []
    for idx, row in agg.iterrows():
        style = norm(row['Style']) if 'Style' in row else ''
        color = norm(row['Color']) if 'Color' in row else ''
        matched = (style, color) in order_style_color
        match_type = 'exact' if matched else 'none'
        match_results.append({
            'Customer_PO': row['Customer_PO'] if 'Customer_PO' in row else np.nan,
            'Style': row['Style'] if 'Style' in row else '',
            'Color': row['Color'] if 'Color' in row else '',
            'Shipping_Method': row['Shipping_Method'] if 'Shipping_Method' in row else '',
            'Shipped_Date': row['Shipped_Date'] if 'Shipped_Date' in row else '',
            'Qty': row['Qty'] if 'Qty' in row else (row['Count'] if 'Count' in row else np.nan),
            'Matched': matched,
            'Match_Type': match_type
        })
    match_df = pd.DataFrame(match_results)
    print("\n--- STYLE+COLOR MATCH OUTPUT DATAFRAME ---")
    print(f"DataFrame shape: {match_df.shape}")
    if match_df.empty:
        print("No shipment rows to match. DataFrame is empty.")
    else:
        print(match_df.head(20))
    # Optionally, save to CSV
    match_df.to_csv("greyson_po4755_style_color_match.csv", index=False)
    print(f"Results saved to greyson_po4755_style_color_match.csv")

# Call the main function when script is run directly
if __name__ == "__main__":
    main()
