import requests, json, pandas as pd
from ruamel.yaml import YAML
from pathlib import Path
from datetime import datetime

CFG = YAML(typ="safe").load((Path(__file__).parent.parent / "config" / "config.yaml").read_text())
LLM_CFG = CFG["llm_analysis"]  # Use separate analysis config

def analyze_reconciliation_patterns(customer: str, results_data: dict, date_range: str = None):
    """
    Analyze reconciliation results using LLM with PO-based batching to avoid timeouts.
    """
    
    exact_matches = results_data.get("exact_matches", pd.DataFrame())
    fuzzy_matches = results_data.get("fuzzy_matches", pd.DataFrame())
    unmatched = results_data.get("unmatched", pd.DataFrame())
    orders = results_data.get("orders", pd.DataFrame())
    
    # Prepare analysis data
    total_shipments = len(exact_matches) + len(fuzzy_matches) + len(unmatched)
    match_rate = (len(exact_matches) + len(fuzzy_matches)) / total_shipments * 100 if total_shipments > 0 else 0
    
    # Create analysis summary
    analysis_summary = {
        "customer": customer,
        "date_range": date_range,
        "total_shipments": total_shipments,
        "exact_matches": len(exact_matches),
        "fuzzy_matches": len(fuzzy_matches),
        "unmatched": len(unmatched),
        "match_rate": match_rate
    }
    
    print(f"🧠 LLM Pattern Analysis for {customer}")
    print(f"   Analyzing {total_shipments} shipments with {match_rate:.1f}% match rate")
    
    # For large datasets, use PO-based batching
    if len(unmatched) > 10:
        print(f"   📦 Large dataset detected ({len(unmatched)} unmatched), using PO-based batching...")
        return analyze_by_po_batches(customer, results_data, analysis_summary, date_range)
    else:
        print(f"   📄 Small dataset, using single analysis...")
        return analyze_single_batch(customer, results_data, analysis_summary, date_range)

def analyze_by_po_batches(customer: str, results_data: dict, analysis_summary: dict, date_range: str):
    """Group unmatched shipments by PO and analyze each batch separately"""
    
    unmatched = results_data.get("unmatched", pd.DataFrame())
    orders = results_data.get("orders", pd.DataFrame())
    
    # Group unmatched shipments by PO NUMBER
    if 'PO NUMBER' not in unmatched.columns:
        print("   ❌ No PO NUMBER column found in unmatched data")
        return None
    
    po_groups = unmatched.groupby('PO NUMBER')
    po_batches = list(po_groups)
    
    print(f"   📦 Processing {len(po_batches)} PO batches...")
    
    # Analyze each PO batch
    batch_results = []
    for po_number, po_shipments in po_batches:
        print(f"   📋 Analyzing PO {po_number} ({len(po_shipments)} shipments)...")
        
        # Create batch-specific results data
        batch_data = {
            "exact_matches": pd.DataFrame(),
            "fuzzy_matches": pd.DataFrame(), 
            "unmatched": po_shipments,
            "orders": orders
        }
        
        # Create batch-specific date range identifier
        batch_date_range = f"{date_range} - PO {po_number}"
        
        # Analyze this batch
        batch_result = analyze_single_batch(customer, batch_data, analysis_summary, batch_date_range)
        if batch_result:
            batch_results.append({
                "po_number": po_number,
                "shipment_count": len(po_shipments),
                "analysis": batch_result["analysis"]
            })
    
    print(f"   ✅ LLM batch analysis completed for {len(batch_results)} POs")
    
    # Combine results into comprehensive report
    combined_analysis = combine_po_analyses(batch_results, analysis_summary)
    
    # Generate combined markdown report
    markdown_content = generate_batched_markdown_report(customer, analysis_summary, combined_analysis, date_range, len(po_batches))
    report_path = save_analysis_report(customer, markdown_content, analysis_summary)
    
    print(f"📄 Combined analysis report: {report_path}")
    
    return {
        "batch_results": batch_results,
        "combined_analysis": combined_analysis,
        "summary": analysis_summary,
        "report_path": report_path
    }

def filter_relevant_cols(df):
    """Filter DataFrame to relevant columns for analysis"""
    if df.empty:
        return df
    
    target_cols = ['PO NUMBER', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION', 'PLANNED DELIVERY METHOD']
    available_cols = [col for col in target_cols if col in df.columns]
    return df[available_cols] if available_cols else df

def analyze_single_batch(customer: str, results_data: dict, analysis_summary: dict, date_range: str):
    """Analyze a single batch of reconciliation results using LLM"""
    
    exact_matches = results_data.get("exact_matches", pd.DataFrame())
    fuzzy_matches = results_data.get("fuzzy_matches", pd.DataFrame())
    unmatched = results_data.get("unmatched", pd.DataFrame())
    orders = results_data.get("orders", pd.DataFrame())
    
    total_shipments = analysis_summary.get("total_shipments", len(unmatched))
    match_rate = analysis_summary.get("match_rate", 0.0)
    
    # Sample the data for LLM analysis (first 5 rows each)
    unmatched_sample = filter_relevant_cols(unmatched.head(5) if not unmatched.empty else pd.DataFrame())
    
    # Get orders that have matching PO numbers to unmatched shipments
    if not unmatched.empty and not orders.empty and 'PO NUMBER' in unmatched.columns and 'PO NUMBER' in orders.columns:
        matching_pos = unmatched['PO NUMBER'].unique()
        matching_orders = orders[orders['PO NUMBER'].isin(matching_pos)]
        orders_sample = filter_relevant_cols(matching_orders.head(5))
    else:
        orders_sample = filter_relevant_cols(orders.head(5) if not orders.empty else pd.DataFrame())
    
    print(f"   🔍 Debug: Unmatched sample shape: {unmatched_sample.shape}")
    print(f"   🔍 Debug: Orders sample shape: {orders_sample.shape}")
    if not unmatched_sample.empty:
        print(f"   🔍 Debug: Sample unmatched shipments:\\n{unmatched_sample.head(3)}")
    if not orders_sample.empty:
        print(f"   🔍 Debug: Sample matching orders:\\n{orders_sample.head(3)}")
    
    # DEBUG: Show exactly what data we're sending to LLM
    print("\\n" + "="*80)
    print("🔍 EXACT DATA BEING SENT TO LLM:")
    print("="*80)
    print("\\nUNMATCHED SHIPMENTS DATA:")
    if not unmatched_sample.empty:
        print(unmatched_sample[['PO NUMBER', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION', 'PLANNED DELIVERY METHOD']].to_string())
    else:
        print("No unmatched data")
    
    print("\\nORDERS DATA:")
    if not orders_sample.empty:
        print(orders_sample[['PO NUMBER', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION', 'PLANNED DELIVERY METHOD']].to_string())
    else:
        print("No matching orders found")
    
    # Analyze unmatched pairs for mapping suggestions
    unmatched_pairs = []
    if not unmatched_sample.empty and not orders_sample.empty:
        # Extract mismatched pairs for the same PO
        po_numbers = set(unmatched_sample['PO NUMBER'].unique()) & set(orders_sample['PO NUMBER'].unique())
        for po in po_numbers:
            shipment_rows = unmatched_sample[unmatched_sample['PO NUMBER'] == po]
            order_rows = orders_sample[orders_sample['PO NUMBER'] == po]
            
            for _, ship_row in shipment_rows.iterrows():
                for _, order_row in order_rows.iterrows():
                    # Compare each field and collect mismatches
                    for col in ['PLANNED DELIVERY METHOD', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION']:
                        ship_val = str(ship_row[col])
                        order_val = str(order_row[col])
                        if ship_val != order_val:
                            unmatched_pairs.append((col, order_val, ship_val))
    
    print(f"\\n🔍 IDENTIFIED MISMATCHED PAIRS: {len(unmatched_pairs)}")
    for col, order_val, ship_val in unmatched_pairs[:5]:  # Show first 5
        print(f"   {col}: '{order_val}' (order) vs '{ship_val}' (shipment)")
    
    print("="*80)
    
    # Create detailed analysis prompt with mapping context
    prompt = {
        "role": "user", 
        "content": f"""Analyze order-shipment reconciliation for {customer}.

DATA SOURCE CONTEXT:
- ORDERS: Customer orders from Excel files (business requirements)
- SHIPMENTS: Customer orders that have been shipped from FileMaker shipping system (fulfillment records)

COLUMN MAPPING (Orders -> Shipments):
- PO NUMBER -> Customer_PO  
- CUSTOMER STYLE -> Style
- CUSTOMER COLOUR DESCRIPTION -> Color  
- PLANNED DELIVERY METHOD -> Shipping_Method

MATCHING CRITERIA:
Our system attempts to match shipments to orders using these 4 fields:
1. PO NUMBER (must match exactly)
2. CUSTOMER STYLE (must match exactly) 
3. CUSTOMER COLOUR DESCRIPTION (must match exactly)
4. PLANNED DELIVERY METHOD (often mismatches due to system differences)

CURRENT RESULTS:
- {total_shipments} shipments processed
- {match_rate:.1f}% match rate ({len(exact_matches)} exact, {len(fuzzy_matches)} fuzzy)
- {len(unmatched)} unmatched shipments

SAMPLE UNMATCHED SHIPMENTS (from FileMaker shipping system):
{unmatched_sample[['PO NUMBER', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION', 'PLANNED DELIVERY METHOD']].to_string() if not unmatched_sample.empty else 'No unmatched data'}

ORDERS FOR SAME PO NUMBERS (from Excel order files):
{orders_sample[['PO NUMBER', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION', 'PLANNED DELIVERY METHOD']].to_string() if not orders_sample.empty else 'No matching orders found'}

IDENTIFIED MISMATCHED VALUE PAIRS:
{chr(10).join([f"- {col}: '{order_val}' (orders) vs '{ship_val}' (shipments)" for col, order_val, ship_val in unmatched_pairs[:10]])}

ANALYSIS NEEDED:
1. Compare the 4 matching fields between shipments and orders
2. Identify which fields are preventing matches (focus on the mismatched pairs above)
3. Suggest specific value mappings to improve matching (e.g., "FAST BOAT" -> "SEA-FB")
4. Provide confidence levels for each suggested mapping
5. Remember these come from different systems (Excel vs FileMaker) so expect formatting differences

Please provide your analysis in TWO parts:

**PART 1: DETAILED ANALYSIS (for human review)**
Provide detailed markdown analysis with reasoning, patterns, and business insights. Use <think> tags to show your reasoning process.

**PART 2: STRUCTURED MAPPINGS (for automation)**
At the end of your response, provide a JSON object with suggested mappings in this exact format:
```json
{{
  "suggested_mappings": [
    {{
      "column": "PLANNED DELIVERY METHOD",
      "order_value": "FAST BOAT",
      "shipment_value": "SEA-FB", 
      "confidence": 0.95,
      "rationale": "Both refer to fast boat delivery method, just different naming conventions between systems"
    }}
  ]
}}
```"""
    }
    
    print(f"   Prompt length: {len(prompt['content'])} characters")
    
    # Simple request with reasoning-focused system message
    body = {
        "model": LLM_CFG["model"], 
        "temperature": 0.3,  # Lower temperature for more focused analysis
        "max_tokens": 3000,  # Increased for full analysis
        "messages": [
            {
                "role": "system",
                "content": "You are a business data analyst specializing in data integration between disparate systems. Analyze data patterns and provide actionable mapping suggestions with confidence levels. Focus on practical solutions for reconciling values between Excel order files and FileMaker shipping systems."
            },
            prompt
        ]
    }
    
    try:
        print("   📡 Sending analysis request to LLM...")
        print(f"   🔍 Debug: Request URL: {LLM_CFG['url']}")
        print(f"   🔍 Debug: Model: {LLM_CFG['model']}")
        print(f"   🔍 Debug: Prompt preview: {prompt['content'][:200]}...")
        r = requests.post(LLM_CFG["url"], json=body, timeout=300)  # Increased to 5 minutes
        r.raise_for_status()
    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to LLM service at {LLM_CFG['url']}")
        return None
    except requests.exceptions.Timeout:
        print(f"❌ LLM analysis request timed out after 300 seconds")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"❌ LLM service returned HTTP error: {e}")
        return None
    
    response_data = r.json()
    print(f"   🔍 Debug: Raw LLM response keys: {list(response_data.keys())}")
    
    if "choices" in response_data and response_data["choices"]:
        content = response_data["choices"][0]["message"]["content"]
        print(f"   🔍 Debug: Response content preview: {content[:200]}...")
        
        # Extract markdown analysis and JSON mappings
        markdown_analysis, json_mappings = extract_analysis_and_mappings(content)
        
        # Save JSON mappings if found
        json_path = None
        if json_mappings:
            json_path = save_mapping_suggestions(customer, json_mappings, analysis_summary)
            print(f"   📄 JSON mappings saved: {json_path}")
        
        # For single batch, create simple markdown report
        if "PO" not in date_range:  # Not a PO batch, generate full report
            markdown_content = generate_simple_markdown_report(customer, analysis_summary, markdown_analysis, date_range)
            report_path = save_analysis_report(customer, markdown_content, analysis_summary)
            print("✅ LLM analysis completed successfully")
            print(f"📄 Analysis report: {report_path}")
            
            return {
                "analysis": markdown_analysis,
                "json_mappings": json_mappings,
                "summary": analysis_summary,
                "report_path": report_path,
                "json_path": json_path
            }
        else:
            # For PO batch, just return the analysis content
            return {
                "analysis": markdown_analysis,
                "json_mappings": json_mappings,
                "summary": analysis_summary
            }
    else:
        print(f"❌ Unexpected LLM response format. Available keys: {list(response_data.keys())}")
        return None


def extract_analysis_and_mappings(content: str):
    """Extract markdown analysis and JSON mappings from LLM response"""
    import re
    
    # Try to find JSON block
    json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
    if json_match:
        try:
            json_mappings = json.loads(json_match.group(1))
            # Remove JSON block from markdown content
            markdown_analysis = re.sub(r'```json\s*\{.*?\}\s*```', '', content, flags=re.DOTALL).strip()
        except json.JSONDecodeError:
            print("   ⚠️ Found JSON block but failed to parse")
            json_mappings = None
            markdown_analysis = content
    else:
        # No JSON found, return full content as markdown
        json_mappings = None
        markdown_analysis = content
    
    return markdown_analysis, json_mappings


def save_mapping_suggestions(customer: str, json_mappings: dict, summary: dict) -> str:
    """Save JSON mapping suggestions to file"""
    
    # Create reports directory structure
    project_root = Path(__file__).parent.parent
    mappings_dir = project_root / "reports" / customer / "mapping_suggestions"
    mappings_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    date_range = summary.get("date_range", "unknown")
    report_id = date_range.replace(" ", "_").replace("shipped_", "") if date_range else "unknown"
    filename = f"mappings_{report_id}_{timestamp}.json"
    
    json_path = mappings_dir / filename
    
    # Add metadata to JSON
    enhanced_mappings = {
        "metadata": {
            "customer": customer,
            "generated": datetime.now().isoformat(),
            "analysis_period": date_range,
            "total_shipments": summary.get("total_shipments", 0),
            "match_rate": summary.get("match_rate", 0)
        },
        "mappings": json_mappings
    }
    
    # Save JSON file
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(enhanced_mappings, f, indent=2)
    
    return json_path


def generate_simple_markdown_report(customer, summary, llm_analysis, date_range):
    """Generate a simple markdown report from LLM analysis"""
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_id = date_range.replace(" ", "_").replace("shipped_", "") if date_range else "unknown"
    
    markdown = f"""# {customer} Reconciliation Analysis Report

**Generated:** {timestamp}  
**Analysis Period:** {date_range}  
**Report ID:** {report_id}

## Performance Overview
- **Total Shipments:** {summary.get('total_shipments', 0)}
- **Match Rate:** {summary.get('match_rate', 0):.1f}%
- **Exact Matches:** {summary.get('exact_matches', 0)}
- **Fuzzy Matches:** {summary.get('fuzzy_matches', 0)}
- **Unmatched:** {summary.get('unmatched', 0)}

## LLM Analysis

{llm_analysis}

---
*This analysis was generated by an AI system. Please review and validate recommendations before implementing.*
"""
    return markdown

def combine_po_analyses(batch_results, analysis_summary):
    """Combine multiple PO batch analyses into a summary"""
    
    total_pos = len(batch_results)
    total_batch_shipments = sum(batch["shipment_count"] for batch in batch_results)
    
    combined = f"""### Analysis Summary ({total_pos} PO batches processed)

"""
    
    for batch in batch_results:
        po_number = batch["po_number"]
        shipment_count = batch["shipment_count"]
        analysis = batch["analysis"]
        
        combined += f"""#### PO {po_number} ({shipment_count} shipments)
{analysis}

---

"""
    
    return combined

def generate_batched_markdown_report(customer, summary, combined_analysis, date_range, num_batches):
    """Generate markdown report for batched analysis"""
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_id = date_range.replace(" ", "_").replace("shipped_", "") if date_range else "unknown"
    
    markdown = f"""# {customer} Reconciliation Analysis Report (PO Batched)

**Generated:** {timestamp}  
**Analysis Period:** {date_range}  
**Report ID:** {report_id}
**Processing Method:** PO-based batching ({num_batches} PO batches)

## Performance Overview
- **Total Shipments:** {summary.get('total_shipments', 0)}
- **Match Rate:** {summary.get('match_rate', 0):.1f}%
- **Exact Matches:** {summary.get('exact_matches', 0)}
- **Fuzzy Matches:** {summary.get('fuzzy_matches', 0)}
- **Unmatched:** {summary.get('unmatched', 0)}

## PO Batch Summary
- **PO Numbers Analyzed:** {num_batches}
- **Shipments in Batches:** {summary.get('unmatched', 0)}

## LLM Analysis by PO

{combined_analysis}

---
*This analysis was generated by an AI system using PO-based batching. Please review and validate recommendations before implementing.*
"""
    return markdown

def save_analysis_report(customer, markdown_content, summary):
    """Save analysis report to file"""
    
    # Create reports directory structure
    project_root = Path(__file__).parent.parent
    reports_dir = project_root / "reports" / customer / "llm_analysis"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename
    date_range = summary.get("date_range", "unknown")
    report_id = date_range.replace(" ", "_").replace("shipped_", "") if date_range else "unknown"
    filename = f"analysis_{report_id}.md"
    
    report_path = reports_dir / filename
    
    # Save report
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    
    return report_path
