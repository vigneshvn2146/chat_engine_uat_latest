"""
LLM utility functions.
"""
import json
import re
from typing import List, Dict, Optional, Tuple
from config import get_llm


def extract_entities(query: str, history: List[tuple], entities: Dict[str, Optional[str]], conversation_summary: str = "") -> Dict[str, Optional[str]]:
    """Uses LLM to extract entities from user query considering historical context and conversation summary."""
    print(f"[DEBUG] Extracting entities from query: {query}")
    
    # Format context in a way that won't trigger Azure's content filters
    context_info = "Previous entities: "
    for key, value in entities.items():
        context_info += f"{key}={value}, "
    
    # Create a simplified context from history
    # Convert history items to a safer format
    safe_history = []
    for i, (user_msg, bot_msg) in enumerate(history[-3:]):
        safe_history.append(f"Exchange {i+1}:\nUser input: {user_msg}\nSystem response: {bot_msg}")
    
    history_text = "\n".join(safe_history)
    
    # Build a more Azure-friendly prompt with enhanced entity extraction
    prompt = f"""Analyze the following conversation about maritime vessels and extract key information.

CONTEXT INFORMATION:
{context_info}

CONVERSATION HISTORY:
{history_text}

MOST RECENT USER INPUT:
"{query}"

Your task is to identify the following information from the current input, considering previous context:
1. Vessel name - if mentioned in current input, use the new value; otherwise, keep previous value
2. IMO number - if mentioned in current input, use the new value; otherwise, keep previous value
3. Equipment system - if mentioned in current input, use the new value; otherwise, keep previous value
4. Engine make - if mentioned in current input, use the new value; otherwise, keep previous value
5. Engine model - if mentioned in current input, use the new value; otherwise, keep previous value
6. Is general query - set to true if the query is asking about engine/equipment without wanting info about a specific vessel
7. Query category: 
   - "deck_side" for queries about navigation, routes, ETA, cargo, or voyages
   - "engine_side" for queries about machinery, maintenance, or technical systems
   - "unknown" if unclear

Format your response as valid JSON with these exact keys: vessel_name, IMO, equipment_system, engine_make, engine_model, is_general_query, category.
Use null for any missing values.
"""

    print(f"Prompt : {prompt}")
    
    try:
        llm = get_llm()
        response = llm.invoke(prompt).content
        
        # Extract just the JSON part (in case there's extra text)
        import re
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        
        if json_match:
            json_str = json_match.group(0)
            try:
                extracted_data = json.loads(json_str)
                print(f"[DEBUG] Successfully parsed JSON")
            except json.JSONDecodeError:
                print(f"ERROR: JSON Decode ERROR after regex extraction")
                extracted_data = {
                    "vessel_name": None, 
                    "IMO": None, 
                    "equipment_system": None, 
                    "engine_make": None,
                    "engine_model": None,
                    "is_general_query": False,
                    "category": "unknown"
                }
        else:
            # Fallback to simple key-value parsing if JSON structure isn't found
            print(f"[DEBUG] No JSON structure found, attempting fallback parsing")
            extracted_data = {
                "vessel_name": None, 
                "IMO": None, 
                "equipment_system": None,
                "engine_make": None,
                "engine_model": None,
                "is_general_query": False,
                "category": "unknown"
            }
            
            # Simple key-value parsing
            for line in response.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip().lower()
                    value = value.strip().strip('"').strip("'")
                    
                    if key == "vessel_name" or key == "vessel name":
                        extracted_data["vessel_name"] = value if value.lower() != "null" else None
                    elif key == "imo" or key == "imo number":
                        try:
                            extracted_data["IMO"] = int(value) if value.lower() != "null" else None
                        except ValueError:
                            extracted_data["IMO"] = None
                    elif key == "equipment_system" or key == "equipment system":
                        extracted_data["equipment_system"] = value if value.lower() != "null" else None
                    elif key == "engine_make" or key == "make":
                        extracted_data["engine_make"] = value if value.lower() != "null" else None
                    elif key == "engine_model" or key == "model":
                        extracted_data["engine_model"] = value if value.lower() != "null" else None
                    elif key == "is_general_query" or key == "general query":
                        extracted_data["is_general_query"] = value.lower() in ["true", "yes", "1"]
                    elif key == "category":
                        extracted_data["category"] = value if value.lower() != "null" else "unknown"
    
    except Exception as e:
        print(f"ERROR: LLM Error: {e}")
        # If Azure filters the content, use a fallback approach
        extracted_data = {
            "vessel_name": None, 
            "IMO": None, 
            "equipment_system": None,
            "engine_make": None,
            "engine_model": None,
            "is_general_query": False,
            "category": "unknown"
        }
        
        # Basic entity extraction logic
        query_lower = query.lower()
        
        # Check for vessel name in new query
        if "vessel" in query_lower and "name" in query_lower:
            words = query_lower.split()
            idx = max(query_lower.find("vessel name"), query_lower.find("vessel"))
            if idx >= 0 and idx + 1 < len(words):
                potential_name = words[idx + 1]
                if potential_name not in ["is", "the", "a", "an"]:
                    extracted_data["vessel_name"] = potential_name
        
        # Preserve existing vessel name if not found in query
        if not extracted_data["vessel_name"] and entities.get("vessel_name"):
            extracted_data["vessel_name"] = entities.get("vessel_name")
            
        # Check for IMO in query
        if "imo" in query_lower:
            import re
            imo_match = re.search(r'imo\s+(\d+)', query_lower)
            if imo_match:
                extracted_data["IMO"] = int(imo_match.group(1))
        
        # Preserve existing IMO if not found in query
        if not extracted_data["IMO"] and entities.get("IMO"):
            extracted_data["IMO"] = entities.get("IMO")
            
        # Check for make/model in query
        if "make" in query_lower:
            words = query_lower.split()
            idx = query_lower.find("make")
            if idx >= 0 and idx + 1 < len(words):
                extracted_data["engine_make"] = words[idx + 1]
        
        if "model" in query_lower:
            words = query_lower.split()
            idx = query_lower.find("model")
            if idx >= 0 and idx + 1 < len(words):
                extracted_data["engine_model"] = words[idx + 1]
        
        # Preserve existing make/model if not found in query
        if not extracted_data["engine_make"] and entities.get("engine_make"):
            extracted_data["engine_make"] = entities.get("engine_make")
        if not extracted_data["engine_model"] and entities.get("engine_model"):
            extracted_data["engine_model"] = entities.get("engine_model")
            
        # Detect if this is a general query
        extracted_data["is_general_query"] = (
            "general" in query_lower or 
            "all" in query_lower or 
            (("any" in query_lower) and not ("vessel" in query_lower)) or
            (("make" in query_lower or "model" in query_lower) and 
             not any(word in query_lower for word in ["vessel", "ship", "imo"]))
        )
            
        # Detect query category based on keywords
        if any(word in query_lower for word in ["eta", "voyage", "speed", "route", "cargo", "weather"]):
            extracted_data["category"] = "deck_side"
        elif any(word in query_lower for word in ["engine", "maintenance", "equipment", "overhaul", "cylinder", "machinery"]):
            extracted_data["category"] = "engine_side"
            
        # Extract equipment system if mentioned
        equipment_keywords = ["engine", "cylinder", "liner", "piston", "turbocharger", "generator", "boiler", "pump"]
        for keyword in equipment_keywords:
            if keyword in query_lower:
                extracted_data["equipment_system"] = keyword
                break
                
        # Preserve existing equipment system if not found in query
        if not extracted_data["equipment_system"] and entities.get("equipment_system"):
            extracted_data["equipment_system"] = entities.get("equipment_system")
    
    print(f"[DEBUG] Extracted Entities: {extracted_data}")
    return extracted_data