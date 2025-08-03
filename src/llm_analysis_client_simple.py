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
    
    # Create summary statistics
    analysis_summary = {
        "customer": customer,
        "date_range": date_range or "unspecified",
        "total_shipments": total_shipments,
        "exact_matches": len(exact_matches),
        "fuzzy_matches": len(fuzzy_matches),
        "unmatched_shipments": len(unmatched),
        "match_rate": round(match_rate, 2),
        "analysis_date": datetime.now().isoformat()
    }
    
    print(f"🧠 LLM Pattern Analysis for {customer}")
    print(f"   Analyzing {total_shipments} shipments with {match_rate:.1f}% match rate")
    
    # Check if we should use PO batching for large datasets
    if len(unmatched) > 10:
        print(f"   📦 Large dataset detected ({len(unmatched)} unmatched), using PO-based batching...")
        return analyze_by_po_batches(customer, results_data, analysis_summary, date_range)
    else:
        print(f"   📄 Small dataset, analyzing as single batch...")
        return analyze_single_batch(customer, results_data, analysis_summary, date_range)


def analyze_by_po_batches(customer: str, results_data: dict, analysis_summary: dict, date_range: str):
    """
    Analyze large datasets by grouping into PO-based batches to avoid timeouts.
    """
    unmatched = results_data.get("unmatched", pd.DataFrame())
    orders = results_data.get("orders", pd.DataFrame())
    
    if unmatched.empty or 'PO NUMBER' not in unmatched.columns:
        print("   ❌ No unmatched data or PO NUMBER column missing")
        return None
    
    # Group by PO NUMBER
    po_groups = unmatched.groupby('PO NUMBER')
    all_analyses = []
    
    print(f"   📦 Processing {len(po_groups)} PO batches...")
    
    for po_number, po_shipments in po_groups:
        print(f"   📋 Analyzing PO {po_number} ({len(po_shipments)} shipments)...")
        
        # Create batch-specific results data
        batch_results = {
            "exact_matches": pd.DataFrame(),  # No exact matches in this batch (they're unmatched)
            "fuzzy_matches": pd.DataFrame(),  # No fuzzy matches in this batch  
            "unmatched": po_shipments,
            "orders": orders
        }
        
        # Analyze this batch
        batch_analysis = analyze_single_batch(customer, batch_results, analysis_summary, f"{date_range} - PO {po_number}")
        
        if batch_analysis:
            all_analyses.append({
                "po_number": po_number,
                "shipment_count": len(po_shipments),
                "analysis": batch_analysis["analysis"]
            })
    
    # Combine all analyses into a single report
    combined_analysis = combine_po_analyses(all_analyses)
    
    # Generate combined markdown report
    markdown_content = generate_po_batch_markdown_report(customer, analysis_summary, all_analyses, combined_analysis, date_range)
    
    # Save the combined report
    report_path = save_analysis_report(customer, markdown_content, analysis_summary)
    
    print(f"✅ LLM batch analysis completed for {len(all_analyses)} POs")
    print(f"📄 Combined analysis report: {report_path}")
    
    return {
        "analysis": combined_analysis,
        "summary": analysis_summary,
        "report_path": report_path,
        "po_batches": all_analyses
    }


def analyze_single_batch(customer: str, results_data: dict, analysis_summary: dict, date_range: str):
    """
    Analyze a single batch of reconciliation data (original function logic).
    """
    exact_matches = results_data.get("exact_matches", pd.DataFrame())
    fuzzy_matches = results_data.get("fuzzy_matches", pd.DataFrame())
    unmatched = results_data.get("unmatched", pd.DataFrame())
    orders = results_data.get("orders", pd.DataFrame())
    
    total_shipments = len(exact_matches) + len(fuzzy_matches) + len(unmatched)
    match_rate = (len(exact_matches) + len(fuzzy_matches)) / total_shipments * 100 if total_shipments > 0 else 0
    """
    Analyze reconciliation results using LLM to identify patterns and create insights.
    Simple version without structured output.
    """
    
    exact_matches = results_data.get("exact_matches", pd.DataFrame())
    fuzzy_matches = results_data.get("fuzzy_matches", pd.DataFrame())
    unmatched = results_data.get("unmatched", pd.DataFrame())
    orders = results_data.get("orders", pd.DataFrame())
    
    # Prepare analysis data
    total_shipments = len(exact_matches) + len(fuzzy_matches) + len(unmatched)
    match_rate = (len(exact_matches) + len(fuzzy_matches)) / total_shipments * 100 if total_shipments > 0 else 0
    
    # Create summary statistics
    analysis_summary = {
        "customer": customer,
        "date_range": date_range or "unspecified",
        "total_shipments": total_shipments,
        "exact_matches": len(exact_matches),
        "fuzzy_matches": len(fuzzy_matches),
        "unmatched_shipments": len(unmatched),
        "match_rate": round(match_rate, 2),
        "analysis_date": datetime.now().isoformat()
    }
    
    print(f"🧠 LLM Pattern Analysis for {customer}")
    print(f"   Analyzing {total_shipments} shipments with {match_rate:.1f}% match rate")
    
    # Prepare structured, grouped data for LLM analysis
    analysis_columns = [
        'PO NUMBER', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION', 
        'PLANNED DELIVERY METHOD', 'ORDER TYPE', 'Customer', 'Shipped_Date'
    ]
    
    def filter_relevant_cols(df):
        if df.empty:
            return df
        return df[[col for col in analysis_columns if col in df.columns]]
    
    # Group unmatched shipments by PO and date for structured analysis
    unmatched_sample = filter_relevant_cols(unmatched.head(10) if not unmatched.empty else pd.DataFrame())
    
    # Find orders that match the PO numbers from unmatched shipments (this is key!)
    if not unmatched.empty and not orders.empty and 'PO NUMBER' in unmatched.columns:
        unmatched_pos = unmatched['PO NUMBER'].unique()
        matching_orders = orders[orders['PO NUMBER'].isin(unmatched_pos)] if 'PO NUMBER' in orders.columns else pd.DataFrame()
        orders_sample = filter_relevant_cols(matching_orders.head(10))
    else:
        orders_sample = filter_relevant_cols(orders.head(5) if not orders.empty else pd.DataFrame())
    
    # Debug: Print actual data being analyzed
    print(f"   🔍 Debug: Unmatched sample shape: {unmatched_sample.shape}")
    print(f"   🔍 Debug: Orders sample shape: {orders_sample.shape}")
    if not unmatched_sample.empty:
        print(f"   🔍 Debug: Sample unmatched shipments:\n{unmatched_sample.head(3)}")
    if not orders_sample.empty:
        print(f"   🔍 Debug: Sample matching orders:\n{orders_sample.head(3)}")
    
    # Create detailed analysis prompt with matching context
    prompt = {
        "role": "user", 
        "content": f"""Analyze order-shipment reconciliation for {customer}.

MATCHING CRITERIA:
Our system attempts to match shipments to orders using:
1. PO NUMBER (must match exactly)
2. CUSTOMER STYLE (must match exactly) 
3. CUSTOMER COLOUR DESCRIPTION (must match exactly)
4. PLANNED DELIVERY METHOD (often mismatches)

CURRENT RESULTS:
- {total_shipments} shipments processed
- {match_rate:.1f}% match rate ({len(exact_matches)} exact, {len(fuzzy_matches)} fuzzy)
- {len(unmatched)} unmatched shipments

SAMPLE UNMATCHED SHIPMENTS:
{unmatched_sample[['PO NUMBER', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION', 'PLANNED DELIVERY METHOD']].to_string() if not unmatched_sample.empty else 'No unmatched data'}

ORDERS FOR SAME PO NUMBERS:
{orders_sample[['PO NUMBER', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION', 'PLANNED DELIVERY METHOD']].to_string() if not orders_sample.empty else 'No matching orders found'}

ANALYSIS NEEDED:
1. Compare the 4 matching fields between shipments and orders
2. Identify which fields are preventing matches (likely delivery method)
3. Suggest specific mappings or rules to improve matching
4. Focus on patterns you can see in the actual data above"""
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
                "content": "You are a business data analyst. Your job is to analyze data patterns and provide actionable business insights. Think through the problem step by step and provide clear, practical recommendations."
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
        
        # Create simple markdown report
        markdown_content = generate_simple_markdown_report(customer, analysis_summary, content, date_range)
        
        # Save the markdown report
        report_path = save_analysis_report(customer, markdown_content, analysis_summary)
        
        print("✅ LLM analysis completed successfully")
        print(f"📄 Analysis report: {report_path}")
        
        return {
            "analysis": content,
            "summary": analysis_summary,
            "report_path": report_path
        }
    else:
        print(f"❌ Unexpected LLM response format. Available keys: {list(response_data.keys())}")
        return None


def generate_simple_markdown_report(customer, summary, llm_analysis, date_range):
    """Generate a simple markdown report from LLM analysis"""
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_id = date_range.replace(" ", "_").replace("shipped_", "") if date_range else "unknown"
    
    markdown = f"""# {customer} Reconciliation Analysis Report

**Generated:** {timestamp}  
**Analysis Period:** {date_range}  
**Report ID:** {report_id}

## Performance Overview
- **Total Shipments:** {summary['total_shipments']}
- **Match Rate:** {summary['match_rate']}%
- **Exact Matches:** {summary['exact_matches']}
- **Fuzzy Matches:** {summary['fuzzy_matches']}
- **Unmatched:** {summary['unmatched_shipments']}

## LLM Analysis

{llm_analysis}

---
*This analysis was generated by an AI system. Please review and validate recommendations before implementing.*
"""
    
    return markdown


def save_analysis_report(customer, markdown_content, analysis_summary):
    """Save the analysis report to file"""
    
    # Create reports directory structure
    reports_base = Path(CFG["report_root"]) / customer / "llm_analysis"
    reports_base.mkdir(parents=True, exist_ok=True)
    
    # Generate filename based on date range
    date_range = analysis_summary.get("date_range", "unknown")
    safe_date_range = date_range.replace(" ", "_").replace("shipped_", "")
    report_filename = f"analysis_{safe_date_range}.md"
    report_path = reports_base / report_filename
    
    # Save markdown report
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    
    return str(report_path)


def combine_po_analyses(all_analyses):
    """
    Combine multiple PO analyses into a single summary.
    """
    if not all_analyses:
        return "No analysis data available."
    
    combined = f"## Summary of {len(all_analyses)} PO Analysis Batches\n\n"
    
    for batch in all_analyses:
        po_number = batch["po_number"]
        shipment_count = batch["shipment_count"]
        analysis = batch["analysis"]
        
        combined += f"### PO {po_number} ({shipment_count} shipments)\n"
        combined += f"{analysis}\n\n"
        combined += "---\n\n"
    
    return combined


def generate_po_batch_markdown_report(customer, summary, all_analyses, combined_analysis, date_range):
    """
    Generate a markdown report for PO-batched analysis.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_id = date_range.replace(" ", "_").replace("shipped_", "") if date_range else "unknown"
    
    total_pos = len(all_analyses)
    total_batch_shipments = sum(batch["shipment_count"] for batch in all_analyses)
    
    markdown = f"""# {customer} Reconciliation Analysis Report (PO Batched)

**Generated:** {timestamp}  
**Analysis Period:** {date_range}  
**Report ID:** {report_id}
**Processing Method:** PO-based batching ({total_pos} PO batches)

## Performance Overview
- **Total Shipments:** {summary['total_shipments']}
- **Match Rate:** {summary['match_rate']}%
- **Exact Matches:** {summary['exact_matches']}
- **Fuzzy Matches:** {summary['fuzzy_matches']}
- **Unmatched:** {summary['unmatched_shipments']}

## PO Batch Summary
- **PO Numbers Analyzed:** {total_pos}
- **Shipments in Batches:** {total_batch_shipments}

## LLM Analysis by PO

{combined_analysis}

---
*This analysis was generated by an AI system using PO-based batching. Please review and validate recommendations before implementing.*
"""
    
    return markdown
