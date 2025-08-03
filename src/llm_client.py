import requests, json, pandas as pd
from ruamel.yaml import YAML
from pathlib import Path

CFG = YAML(typ="safe").load((Path(__file__).parent.parent / "config" / "config.yaml").read_text())["llm"]

def propose_links(orders: pd.DataFrame, ships: pd.DataFrame, sample=None):
    """Ask LM Studio to map unmatched ship rows to order rows."""
    # For daily processing, we work with the actual shipment count (usually small)
    # and limit orders to a reasonable sample to fit context window
    actual_ships = len(ships)
    max_orders_for_context = min(10, len(orders))  # Much smaller sample for faster processing
    
    # Only send relevant columns to LLM to reduce token usage
    relevant_order_cols = [
        'PO NUMBER', 'PLANNED DELIVERY METHOD', 'CUSTOMER STYLE', 
        'CUSTOMER COLOUR DESCRIPTION', 'AAG ORDER NUMBER', 'ORDER TYPE',
        'CUSTOMER NAME', 'ORDER DATE PO RECEIVED'
    ]
    relevant_ship_cols = [
        'PO NUMBER', 'PLANNED DELIVERY METHOD', 'CUSTOMER STYLE',
        'CUSTOMER COLOUR DESCRIPTION', 'Customer', 'Shipped_Date', 'Qty'
    ]
    
    # Filter to only relevant columns that exist
    order_cols = [col for col in relevant_order_cols if col in orders.columns]
    ship_cols = [col for col in relevant_ship_cols if col in ships.columns]
    
    orders_sample = orders[order_cols].head(max_orders_for_context)
    ships_sample = ships[ship_cols]  # Use all shipments (daily chunks are small)
    
    print(f"🧠 LLM Daily Analysis:")
    print(f"   Analyzing {len(ships_sample)} shipments against {len(orders_sample)} orders")
    print(f"   Using {len(order_cols)} order columns: {order_cols}")
    print(f"   Using {len(ship_cols)} shipment columns: {ship_cols}")
    
    # Create a more focused prompt for daily reconciliation
    prompt = {
        "role": "user",
        "content": (
            "You are a shipping reconciliation expert. Today's shipments need to be matched to existing orders.\n"
            "Analyze the shipments and orders below to find matches.\n"
            "Focus on these key matching criteria:\n"
            "1. PO NUMBER (exact or partial matches)\n"
            "2. CUSTOMER STYLE (product codes/names)\n" 
            "3. CUSTOMER COLOUR DESCRIPTION (colors)\n"
            "4. PLANNED DELIVERY METHOD vs Shipping_Method (shipping methods like 'SEA-FB' might match 'FAST BOAT')\n\n"
            "Output ONLY a JSON array of match objects:\n"
            "[{\"shipment_index\":0, \"order_index\":5, \"confidence\":0.95, \"reason\":\"PO+Style+Color match\"}]\n"
            "Only include matches with confidence ≥ 0.85.\n\n"
            f"ORDERS ({len(orders_sample)} available):\n{orders_sample.to_json(orient='records', indent=1)}\n\n"
            f"SHIPMENTS ({len(ships_sample)} to match):\n{ships_sample.to_json(orient='records', indent=1)}"
        )
    }
    
    print(f"   Prompt length: {len(prompt['content'])} characters")
    print(f"   Expected: JSON array with shipment_index, order_index, confidence, reason")
    body = {"model": CFG["model"], "messages":[prompt]}
    
    try:
        r = requests.post(CFG["url"], json=body, timeout=120)  # Increased timeout
        r.raise_for_status()
    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to LLM service at {CFG['url']}")
        print("   Make sure LM Studio is running and accessible")
        return []
    except requests.exceptions.Timeout:
        print(f"❌ LLM request timed out after 120 seconds")
        print("   The model may be processing a complex request")
        print("   Try reducing the sample size or using a faster model")
        return []
    except requests.exceptions.HTTPError as e:
        print(f"❌ LLM service returned HTTP error: {e}")
        if r.text and "context" in r.text.lower():
            print(f"   Response text: {r.text}")
            print("   💡 Context window issue detected:")
            print("      - Your model supports 1M tokens but LM Studio is using only 4096")
            print("      - In LM Studio, go to Settings and increase 'Context Length' to use more of the model's capacity")
            print("      - Or reduce sample size in the code")
        print(f"   Request payload size: {len(str(body))} characters")
        return []
    
    # Debug: Print the actual response to understand its structure
    response_data = r.json()
    
    # Handle error responses from the API
    if "error" in response_data:
        print(f"❌ LLM API Error: {response_data['error']}")
        print(f"   URL: {CFG['url']}")
        print(f"   Model: {CFG['model']}")
        print("   Common fixes:")
        print("   - Check if LM Studio is running")
        print("   - Verify the correct endpoint URL (try /v1/chat/completions instead)")
        print("   - Check if the model is loaded in LM Studio")
        return []
    
    # Handle different possible response formats
    if "choices" in response_data:
        # Standard OpenAI format
        content = response_data["choices"][0]["message"]["content"]
    elif "response" in response_data:
        # Alternative format some APIs use
        content = response_data["response"]
    elif "message" in response_data:
        # Direct message format
        content = response_data["message"]
    elif "content" in response_data:
        # Direct content format
        content = response_data["content"]
    else:
        # If we can't find the content, print keys and return empty
        print(f"❌ Unexpected LLM response format. Available keys: {list(response_data.keys())}")
        print(f"   Response: {response_data}")
        return []
    
    try:
        parsed_response = json.loads(content)
        print(f"✅ LLM found {len(parsed_response)} potential matches")
        for match in parsed_response:
            print(f"   Ship {match.get('shipment_index')} → Order {match.get('order_index')} "
                  f"(conf: {match.get('confidence'):.2f}) - {match.get('reason', 'no reason')}")
        return parsed_response
    except json.JSONDecodeError as e:
        print(f"❌ Could not parse LLM response as JSON: {e}")
        print(f"   Raw content: {content[:500]}...")  # Show first 500 chars
        # Try to extract JSON from markdown code blocks
        import re
        json_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', content, re.DOTALL)
        if json_match:
            try:
                parsed_response = json.loads(json_match.group(1))
                print(f"✅ Extracted JSON from markdown: {len(parsed_response)} matches")
                return parsed_response
            except json.JSONDecodeError:
                pass
        return []
