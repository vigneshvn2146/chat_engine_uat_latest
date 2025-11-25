"""
LLM Client Module - UPDATED VERSION with Signed URL Support - FIXED IMPORT ISSUE

Handles all interactions with Large Language Models (Mistral, Llama) and embedding models.
Provides a unified interface for AI model interactions.
PRODUCTION-READY with robust error handling and signed URL replacement
"""

import re, json, os
import boto3
import json
import logging
import re
import traceback
from typing import List, Dict, Any, Optional, Union
from botocore.exceptions import BotoCoreError, ClientError
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger(__name__)

class LLMClient:
    """Unified client for all LLM interactions"""
    
    def __init__(self):
        """Initialize LLM client with AWS Bedrock"""
        # Lazy import to avoid circular dependencies
        from config import config
        
        self.aws_credentials = config.get_aws_credentials()
        self.model_configs = config.get_model_configs()
        self.config = config  # Store config reference
        self._initialize_bedrock_client()
    
    def _initialize_bedrock_client(self):
        """Initialize AWS Bedrock client"""
        try:
            self.bedrock_runtime = boto3.client(
                service_name="bedrock-runtime",
                **self.aws_credentials
            )
            logger.info("✅ Bedrock client initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Bedrock client: {str(e)}")
            self.bedrock_runtime = None

    @staticmethod
    def extract_data_json(final_text: str):
        """
        Extract structured data from LLM output.

        - Keeps previous behavior for ```json``` fenced blocks (charts, list-of-dicts tables, explicit table objects).
        - Also parses Markdown pipe tables (header + separator) found outside code fences and returns them as:
        { "type": "table", "data": [ {...}, {...} ] }

        Returns: list of structured objects, e.g.
        [
            { "type": "chart", ... },
            { "type": "table", "data": [...] }
        ]
        """
        data_json = []

        if not final_text:
            return data_json

        # -------------------
        # Pass 1: JSON blocks (```json ... ```)
        # -------------------
        json_matches = re.findall(r"```json\s*(.*?)\s*```", final_text, re.DOTALL | re.IGNORECASE)

        for match in json_matches:
            try:
                normalized = match.strip()
                parsed = json.loads(normalized)

                # ---- Chart Case: only extract charts from JSON blocks (no table JSON handling) ----
                chart_obj = None

                # Case A: nested chart object e.g. { "chart": { "type": "line", ... }, ... }
                if isinstance(parsed, dict) and isinstance(parsed.get("chart"), dict):
                    chart_obj = parsed

                # Case B: direct chart object with keys like "chart_type" or "type"
                elif isinstance(parsed, dict):
                    # Accept when top-level keys indicate a chart
                    top_type = parsed.get("type") or parsed.get("chart_type") or parsed.get("chartType")
                    if isinstance(top_type, str) and top_type.strip().lower() in {"line", "bar", "pie", "doughnut", "radar", "scatter", "chart"}:
                        chart_obj = parsed

                # If we found a chart-like object, normalize and append
                if chart_obj:
                    # Determine chart_type from many possible locations, normalize to lowercase
                    raw_chart_type = (
                        (chart_obj.get("chart") or {}).get("type")
                        or chart_obj.get("chart_type")
                        or chart_obj.get("chartType")
                        or chart_obj.get("type")
                        or "chart"
                    )
                    chart_type = str(raw_chart_type).strip().lower()

                    # Title may live nested or top-level
                    title = (chart_obj.get("chart") or {}).get("title") or chart_obj.get("title") or ""

                    # Labels: xAxis.categories or labels
                    # Labels: xAxis.categories or labels or Chart.js "data.labels"
                    labels = (
                        (chart_obj.get("xAxis") or {}).get("categories")
                        or chart_obj.get("labels")
                        or (chart_obj.get("data") or {}).get("labels")
                        or []
                    )

                    data_points = []
                    legend = ""

                    # Case 1: Highcharts-style "series"
                    if isinstance(chart_obj.get("series"), list) and chart_obj["series"]:
                        first_series = chart_obj["series"][0]
                        if isinstance(first_series, dict):
                            data_points = first_series.get("data", [])
                            legend = first_series.get("name", "") or chart_obj.get("legend", "")

                    # Case 2: Chart.js-style "datasets"
                    elif isinstance(chart_obj.get("data"), dict) and isinstance(chart_obj["data"].get("datasets"), list):
                        # Pick first dataset for consistency
                        first_dataset = chart_obj["data"]["datasets"][0]
                        data_points = first_dataset.get("data", [])
                        legend = first_dataset.get("label", "")

                    # Case 3: Flat "data" array
                    elif "data" in chart_obj and isinstance(chart_obj.get("data"), list):
                        data_points = chart_obj.get("data", [])
                        legend = chart_obj.get("legend", "")


                    # Ensure numeric values remain as-is; if strings, leave them (consumer can coerce)
                    formatted = {
                        "data": data_points,
                        "type": "chart",
                        "chart_type": chart_type,
                        "title": title,
                        "labels": labels,
                        "legend": legend
                    }
                    data_json.append(formatted)

                # Otherwise: ignore non-chart JSON blocks here (Markdown table parsing handled later)
            except json.JSONDecodeError as e:
                logger = logging.getLogger(__name__)
                logger.debug("❌ JSON parse error in extract_data_json (json block): %s", e)
                # continue to next match


        # -------------------
        # Pass 2: Markdown pipe tables (scan text outside fenced code blocks)
        # -------------------

        # remove fenced code blocks but keep markdown tables outside
        parts = re.split(r"```.*?```", final_text, flags=re.DOTALL)
        text_no_code = "\n".join(parts)


        lines = text_no_code.splitlines()
        i = 0
        separator_re = re.compile(r'^\s*\|?\s*(?::?-+:?\s*\|)+\s*$')  # detects header separator like "| --- | --- |" (with optional : for alignment)

        while i < len(lines):
            line = lines[i].rstrip()
            # look for a potential header line that contains pipes
            if '|' in line:
                # next line must be a separator to be a markdown table
                if i + 1 < len(lines) and separator_re.match(lines[i + 1]):
                    # parse header
                    header_line = line.strip()
                    # remove leading/trailing pipe and split
                    header_line = header_line.strip().strip('|')
                    headers = [h.strip() for h in header_line.split('|')]
                    # advance to data rows
                    j = i + 2
                    table_rows = []
                    while j < len(lines):
                        row_line = lines[j].strip()
                        # stop on blank line or line without pipe
                        if row_line == "" or '|' not in row_line:
                            break
                        # skip separator-like rows accidentally present
                        if separator_re.match(row_line):
                            j += 1
                            continue

                        # split row into values
                        row_vals = [v.strip() for v in row_line.strip().strip('|').split('|')]

                        # If row length differs from headers, try to adjust:
                        if len(row_vals) < len(headers):
                            # pad with empty strings
                            row_vals += [''] * (len(headers) - len(row_vals))
                        elif len(row_vals) > len(headers):
                            # if there are extra columns, merge extras into last column
                            row_vals = row_vals[:len(headers)-1] + [" | ".join(row_vals[len(headers)-1:])]

                        # Build dict for this row
                        row_obj = dict(zip(headers, row_vals))
                        table_rows.append(row_obj)
                        j += 1

                    # If we captured at least one data row, append table
                    if table_rows:
                        data_json.append({
                            "type": "table",
                            "data": table_rows
                        })
                        # skip ahead
                        i = j
                        continue
            i += 1
        
        return data_json

    def generate_mistral_response(self, context: str, temperature: float = 0.2, max_tokens: int = 2048) -> str:
        """Generate response using Mistral model"""
        if not self.bedrock_runtime:
            logger.error("❌ Bedrock client not initialized")
            return '{"error": "LLM client not available"}'
        
        try:
            message = {
                "role": "user",
                "content": [{"text": f"<content>{context}</content>"}],
            }

            response = self.bedrock_runtime.converse(
                modelId=self.model_configs['mistral'],
                messages=[message],
                inferenceConfig={
                    "maxTokens": max_tokens,
                    "temperature": temperature
                },
            )

            response_message = response['output']['message']
            response_content = response_message['content']
            
            # Extract text from content blocks
            if isinstance(response_content, list):
                text_blocks = [block.get('text', '') for block in response_content if 'text' in block]
                result = ''.join(text_blocks)
            else:
                result = str(response_content)
            
            logger.debug("✅ Mistral response generated successfully")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in Mistral generation: {str(e)}")
            return f'{{"error": "Mistral generation failed: {str(e)}"}}'

    def generate_claude_response(self, messages, model_id="anthropic.claude-3-sonnet-20240229-v1:0"):
        """Generate response using Claude model with proper error handling"""
        try:
            # Load environment variables
            aws_access_key = os.getenv("prod_aws_access_key_id")
            aws_secret_key = os.getenv("prod_aws_secret_access_key")
            region = os.getenv("prod_aws_region_name")

            # Check for missing variables
            if not all([aws_access_key, aws_secret_key, region]):
                missing = [k for k, v in {
                    "prod_aws_access_key_id": aws_access_key,
                    "prod_aws_secret_access_key": aws_secret_key,
                    "prod_aws_region_name": region
                }.items() if not v]
                raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

            # Create the client
            session = boto3.Session()
            client = session.client(
                "bedrock-runtime",
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region
            )

            # Validate and format messages properly
            if isinstance(messages, str):
                # If a string is passed, convert it to proper message format
                formatted_messages = [
                    {
                        "role": "user",
                        "content": [{"text": messages}]
                    }
                ]
            elif isinstance(messages, list):
                # Validate existing message format
                formatted_messages = []
                for msg in messages:
                    if isinstance(msg, dict) and 'role' in msg and 'content' in msg:
                        # Ensure content is in proper format
                        if isinstance(msg['content'], str):
                            formatted_msg = {
                                "role": msg['role'],
                                "content": [{"text": msg['content']}]
                            }
                        else:
                            formatted_msg = msg
                        formatted_messages.append(formatted_msg)
                    else:
                        raise ValueError(f"Invalid message format: {msg}")
            else:
                raise ValueError(f"Messages must be string or list, got {type(messages)}")

            # Make the request
            logger.info("Sending request to Bedrock with model_id: %s", model_id)
            response = client.converse(modelId=model_id, messages=formatted_messages)

            # Extract and return the text
            content = response['output']['message']['content'][0]['text']
            logger.info("Received response from Bedrock successfully.")
            return content

        except EnvironmentError as env_err:
            logger.error("Environment configuration error: %s", env_err)
            raise

        except (BotoCoreError, ClientError) as aws_err:
            logger.exception("AWS client error occurred.")
            raise

        except KeyError as parse_err:
            logger.error("Unexpected response format: missing key - %s", parse_err)
            raise

        except Exception as e:
            logger.exception("Unexpected error occurred in generate_claude_response.")
            raise        
    
    def generate_llama_response(self, context: str, temperature: float = 0.5, max_gen_len: int = 2048) -> str:
        """Generate response using Llama model"""
        if not self.bedrock_runtime:
            logger.error("❌ Bedrock client not initialized")
            return '{"error": "LLM client not available"}'
        
        try:
            formatted_prompt = f"""
            <|begin_of_text|><|start_header_id|>user<|end_header_id|>
            {context}
            <|eot_id|>
            <|start_header_id|>assistant<|end_header_id|>
            """

            request_payload = json.dumps({
                "prompt": formatted_prompt, 
                "max_gen_len": max_gen_len, 
                "temperature": temperature
            })

            response = self.bedrock_runtime.invoke_model(
                modelId=self.model_configs['llama'], 
                body=request_payload
            )
            
            model_response = json.loads(response["body"].read())
            generated_text = model_response.get("generation", "")
            
            logger.debug("✅ Llama response generated successfully")
            return generated_text
            
        except Exception as e:
            logger.error(f"❌ Error in Llama generation: {str(e)}")
            return f'{{"error": "Llama generation failed: {str(e)}"}}'
    
    def get_embeddings(self, query: str) -> List[float]:
        """Get embeddings for a query using Titan model"""
        if not self.bedrock_runtime:
            logger.error("❌ Bedrock client not initialized")
            return [0.0] * 1024  # Return zero vector as fallback
        
        try:
            body = json.dumps({"inputText": query})
            
            response = self.bedrock_runtime.invoke_model(
                body=body,
                modelId=self.model_configs['titan_embed'],
                accept="application/json",
                contentType="application/json"
            )

            response_body = json.loads(response["body"].read())
            embedding = response_body.get("embedding", [0.0] * 1024)
            
            logger.debug(f"✅ Embeddings generated successfully for query length: {len(query)}")
            return embedding
            
        except Exception as e:
            logger.error(f"❌ Error getting embeddings: {str(e)}")
            return [0.0] * 1024  # Return zero vector as fallback

# 1. IMO – 7-digit International Maritime Organization numbers (e.g., "9234567", "1234567")
# 2. MAKE (string) — Extract the exact equipment manufacturer/brand (e.g., "MAN B&W", "Wärtsilä", "Caterpillar", "Yanmar") and return it exactly as it appears in the text. 
# 3. EQUIPMENT (string) — Equipment type (e.g., "main engine", "generator", "pump", "thruster")
# 4. VESSELNAME (string) — Extract the exact vessel name from the text. A vessel name usually appears as a proper noun, often prefixed by "MV", "MT", "SS", "NCC", "HMS", "FPSO", etc (e.g., "MV Atlantic", "NCC DALIA", "Prelude"). Do not return equipment names, company names, or random words. Return the full vessel name exactly as it appears in the text, including prefixes if present (e.g., "MV", "MT"). If no vessel name is explicitly mentioned, return an empty string.
# 5. MAKETYPE (string) — Manufacturer category when inferable (e.g., "OEM", "third-party", "vendor group"). If not inferable from make, return "".
# 6. DETAIL (string) — Capture the structured numeric/alpha prefix from the model, such as "S60", "S70", "S50", "96C", "3516". This usually represents the cylinder count, bore size, or series designation (e.g., Model "6S50MC" → DETAIL="6S50"). Do not capture the model name directly.
# 7. ENGINETYPE (string) — Capture the model-level engine subtype code such as ME, ME-C, ME-B, MC, MC-C, RT-flex, DF. If both subtype (ME/MC/etc.) and fuel type (diesel, dual-fuel, gas, electric) appear, prefer the subtype for ENGINETYPE (e.g., Model "6S50MC" → ENGINETYPE="MC").
# 8. MODEL (string) — A model is an alphanumeric code used by manufacturers (e.g., "3516", "RT-flex96C", "S60ME-C", "6S70MC-C", "6S50MC"). Extract the exact equipment model designation from the text. If you don't find the model, get the source text of DETAIL.
      
    def extract_entities_with_llm(self, query: str, fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """Extract maritime entities from user query using LLM"""

        if fields is None or not fields:
            fields = ["VESSELNAME", "IMO", "MAKE", "MODEL", "EQUIPMENT", "MAKETYPE", "DETAIL", "ENGINETYPE"]

        # Turn list into bullet string for prompt
        fields_str = "\n".join([f"- {field}" for field in fields])
        
        extraction_prompt = f"""You are a maritime domain expert specializing in entity extraction from user queries.

Your task is to extract ONLY the following fields from the user query with high accuracy:
{fields_str}

1. IMO – 7-digit International Maritime Organization numbers (e.g., "9234567", "1234567")
2. MAKE (string) — Extract the exact equipment manufacturer/brand (e.g., "MAN B&W", "Wärtsilä", "Caterpillar", "Yanmar") and return it exactly as it appears in the text. 
3. EQUIPMENT (string) — Extract the equipment part that the user is speaking or asking about. Don't be generalized, be very specific and extract the exact same from the user query (e.g., "bearing", "valve", "screw", "nut", "ring", "oil", "pipe", "insulation", "fuel pump") etc. When identifying the EQUIPMENT:
    - Always pick the **most specific part** mentioned in the query.
    - If a smaller component (like “fuel pump”, “valve”, “nozzle”, “bearing”, “vit system”, etc.) is present, that becomes the EQUIPMENT — not the larger system it belongs to.
    - Only use general terms (like “engine”, “generator”, “compressor”) if no smaller part is explicitly mentioned.
    - Be precise, not general. Always extract the smallest specific part mentioned (e.g., “fuel pump”, “valve”, “nozzle”, “bearing”, “coupling”, “filter”, “plunger”, “camshaft”, etc.). Do not generalize to “engine”, “system”, or “machine” unless that’s the only thing mentioned.
    - Use the context of the action. The verb in the query often points to the equipment. For example: “adjust”, “set”, “calibrate” → fuel pump, valve, or sensor. “dismantle”, “remove”, “assemble” → the object being dismantled (e.g., “fuel pump barrel assembly”). “inspect”, “clean”, “lubricate” → a part like “filter”, “bearing”, or “piston”
    - If a suffix or prefix exists for a known equipment root word, always capture the entire phrase.
4. VESSELNAME (string) — Extract the exact vessel name from the text. A vessel name usually appears as a proper noun, often prefixed by "MV", "MT", "SS", "NCC", "HMS", "FPSO", etc (e.g., "MV Atlantic", "NCC DALIA", "Prelude"). Do not return equipment names, company names, or random words. Return the full vessel name exactly as it appears in the text, including prefixes if present (e.g., "MV", "MT"). If no vessel name is explicitly mentioned, return an empty string.
5. MAKETYPE (string) — Manufacturer category when inferable (e.g., "OEM", "third-party", "vendor group"). If not inferable from make, return "".
6. DETAIL — DETAIL (string) — Capture the structured numeric/alpha prefix from the model, such as "6S60", "6S70", "6S50", "96C", "3516". This usually represents the cylinder count, bore size, or series designation (e.g., Model "6S50MC" → DETAIL="6S50"). Do not capture the model name directly.
7. ENGINETYPE (string) — Capture the model-level engine subtype code such as ME, ME-C, ME-B, MC, MC-C, RT-flex, DF. If both subtype (ME/MC/etc.) and fuel type (diesel, dual-fuel, gas, electric) appear, prefer the subtype for ENGINETYPE (e.g., Model "6S50MC" → ENGINETYPE="MC"). Ignore suffixes like "-C7", "C-7", "C7".
8. MODEL (string) — A model is an alphanumeric code used by manufacturers (e.g., "3516", "RT-flex96C", "S60ME-C", "6S70MC-C", "6S50MC"). Extract the exact equipment model designation from the text. If you don't find the model, get the source text of DETAIL.

EXTRACTION RULES:
- Extract ONLY if explicitly mentioned in the query
- Be case-insensitive but preserve original casing in output
- Handle common abbreviations (e.g., "Cat" → "Caterpillar")
- Include confidence level for each extraction
- Extract entities if they are clearly stated in the text, even if they appear alongside other words (e.g., "HYUNDAI MAN B&W Auxiliary Engine" → MAKE = "HYUNDAI MAN B&W", EQUIPMENT = "Auxiliary Engine").
- If not explicitly present, leave blank.

RESPONSE FORMAT (JSON only, no other text):
{{
{", ".join([f'  "{field}": {{"value": "<string>", "confidence": <0.0-1.0>, "source_text": "<string>"}}' for field in fields])}
}}

USER QUERY: {query}

Important:
- Return ONE JSON object only.
- Do not add explanations, notes, or any text outside the JSON.
"""

        try:
            raw = self.generate_claude_response(extraction_prompt)
            if not raw or not isinstance(raw, str):
                raise ValueError("Empty or non-string LLM response")

            # Try to clean JSON but be robust: extract the first {...} block if present
            cleaned = self._clean_json_response(raw) if hasattr(self, "_clean_json_response") else raw
            # try to find JSON object inside cleaned
            m = re.search(r'(\{.*\})', cleaned, flags=re.S)
            json_text = m.group(1) if m else cleaned

            result = json.loads(json_text)
            validated = self._validate_entity_extraction_result(result)
            return validated or {k: {"value": "", "confidence": 0.0, "source_text": ""} for k in fields}

        except Exception as e:
            logger.error(f"❌ Exception in LLM entity extraction: {e}")
            # fallback: return empty entities with confidences (implement or reuse your helper)
            try:
                return self._get_empty_entities_with_confidence()
            except Exception:
                # ultimate fallback skeleton
                return {k: {"value": "", "confidence": 0.0, "source_text": ""} for k in fields}
    
    def _clean_json_response(self, response_text: str) -> str:
        """Clean and validate LLM response to ensure valid JSON"""
        if not response_text:
            return "{}"

        # Remove common fenced code markers
        response_text = re.sub(r'```json\s*', '', response_text, flags=re.IGNORECASE)
        response_text = re.sub(r'```(?:\w+)?\s*', '', response_text, flags=re.IGNORECASE)

        # Try to extract the OUTERMOST JSON object
        start = response_text.find('{')
        end = response_text.rfind('}')
        if start != -1 and end != -1 and end > start:
            candidate = response_text[start:end+1]
            try:
                json.loads(candidate)
                return candidate
            except json.JSONDecodeError as e:
                logger.debug("JSON parsing failed on outer object: %s", e)

        logger.error("❌ No valid JSON found in LLM response (first 200 chars): %s", response_text[:200])
        return "{}"

    def _validate_entity_extraction_result(self, result: Dict) -> Dict[str, Any]:
        """Validate and clean extraction result from LLM. Always returns a dict with expected keys."""
        expected_keys = {
            "VESSELNAME", "IMO", "MAKE", "MODEL", "EQUIPMENT",
            "MAKETYPE", "DETAIL", "ENGINETYPE"
        }

        # If result is not a dict, return empty skeleton
        if not isinstance(result, dict):
            return {k: {"value": "", "confidence": 0.0, "source_text": ""} for k in expected_keys}

        cleaned_result = {}

        # Safely read configured threshold; fallback default
        conf_threshold = 0.6
        try:
            conf_threshold = float(getattr(self, "config", None).entity_confidence_threshold)
        except Exception:
            # keep default 0.6 if config or attribute missing or invalid
            conf_threshold = 0.6

        for key in expected_keys:
            item = result.get(key) or {}
            if isinstance(item, dict):
                # safe extraction with fallbacks
                value = str(item.get("value", "") or "").strip()
                try:
                    confidence = float(item.get("confidence", 0.0) or 0.0)
                except Exception:
                    confidence = 0.0
                source_text = str(item.get("source_text", "") or "").strip()

                # clamp confidence
                confidence = max(0.0, min(1.0, confidence))

                # apply threshold using local variable
                if confidence < conf_threshold:
                    value = ""
                    confidence = 0.0

                cleaned_result[key] = {
                    "value": value,
                    "confidence": confidence,
                    "source_text": source_text
                }
            else:
                cleaned_result[key] = {"value": "", "confidence": 0.0, "source_text": ""}

        return cleaned_result
    
    def _get_empty_entities_with_confidence(self) -> Dict[str, Any]:
        """Return empty entity structure with confidence scores"""
        return {
            "VESSELNAME": {"value": "", "confidence": 0.0, "source_text": ""},
            "IMO": {"value": "", "confidence": 0.0, "source_text": ""},
            "MAKE": {"value": "", "confidence": 0.0, "source_text": ""},
            "MODEL": {"value": "", "confidence": 0.0, "source_text": ""},
            "EQUIPMENT": {"value": "", "confidence": 0.0, "source_text": ""},
            "MAKETYPE": {"value": "", "confidence": 0.0, "source_text": ""},
            "DETAIL": {"value": "", "confidence": 0.0, "source_text": ""},
            "ENGINETYPE": {"value": "", "confidence": 0.0, "source_text": ""}
        }

    def classify_query_type(self, query: str, equipment_context: Dict[str, str]) -> str:
        """
        Classify query as NON_MANUALS or MANUAL with robust heuristics + safe LLM fallback.
        Returns either "NON_MANUALS" or "MANUAL".
        """

        # Normalize
        q = (query or "").strip()
        ql = q.lower()

        # Build small equipment context snippet (if present)
        equipment_info = ""
        if equipment_context.get('make') and equipment_context.get('model'):
            equipment_info = f"Equipment: {equipment_context['make']} {equipment_context['model']}"

        # Heuristic keyword sets (tuned for precision)
        non_manual_keywords = {
            "fault", "faults", "failure", "fail", "failed",
            "alarm", "alarms", "vibration", "leak", "smoke",
            "tripped", "overheat", "not firing", "not starting",
            "symptom", "diagnos", "troubleshoot", "rectify",
            "repair", "damage", "broken", "worn", "scratch",
            "guide damaged", "rings broken", "unit not firing"
        }

        manual_keywords = {
            "how to operate", "start", "stop", "operation",
            "maintenance", "inspection", "routine check",
            "periodic", "interval", "schedule", "greasing",
            "settings", "adjusting", "torque values"
        }

        # Quick heuristic scoring
        non_manual_count = sum(1 for kw in non_manual_keywords if kw in ql)
        manual_count = sum(1 for kw in manual_keywords if kw in ql)

        logger.debug(f"Classifier heuristic counts - non_manuals: {non_manual_count}, manual: {manual_count} for query: {q[:120]}")

        # Strict heuristic rules (high precision, prefer MANUAL for ambiguous user intents that include how/steps)
        if manual_count >= 1 and non_manual_count == 0:
            logger.debug("Heuristic -> MANUAL (manual keywords present, no non_manuals keywords)")
            return "MANUAL"
        if non_manual_count >= 2 and manual_count == 0:
            logger.debug("Heuristic -> non_manuals (2+ non_manuals keywords, no manual keywords)")
            return "NON_MANUALS"

        # If heuristics are inconclusive, call the LLM with a constrained few-shot prompt
        try:
            # Few-shot examples to steer the model
            few_shot = (
                'Example 1:\n'
                'User Question: "Engine is tripping on overload and shows alarm code 123"\n'
                'Answer: {"info_type": "NON_MANUALS"}\n\n'
                'Example 2:\n'
                'User Question: "How to change the piston ring on MAN B&W 6S50MC?"\n'
                'Answer: {"info_type": "MANUAL"}\n\n'
                'Example 3:\n'
                'User Question: "There is excessive vibration at 1000 RPM"\n'
                'Answer: {"info_type": "NON_MANUALS"}\n\n'
                'Example 4:\n'
                'User Question: "Procedure for removing turbocharger bearing housing"\n'
                'Answer: {"info_type": "MANUAL"}\n\n'
            )

            prompt = (
                "You are a strict classifier for maritime maintenance queries. "
                "Your job is to determine whether the user's question is related to 'NON_MANUALS' "
                "(Failure Modes, Effects, and Criticality Analysis) or 'MANUAL' "
                "(standard operation, troubleshooting, or procedural reference).\n\n"
                "If you are uncertain, you must default to 'NON_MANUALS' (non-manual)."
                "Return only a JSON object with a single key 'info_type' whose value is either 'NON_MANUALS' or 'MANUAL'. "
                "Do NOT include any explanations, comments, or extra text outside the JSON.\n\n"
                f"{few_shot}\n"
                f"Equipment Context: {equipment_info}\n\n"
                f"User Question: {q}\n\n"
                'Respond with ONLY a valid JSON object, for example: {"info_type": "NON_MANUALS"}'
            )

            # Call the model deterministically (temperature 0)
            raw = self.generate_claude_response(prompt)

            # Defensive: extract first JSON object from model output
            m = re.search(r'\{.*?\}', raw, flags=re.DOTALL)
            if not m:
                logger.warning(f"Classifier LLM returned no JSON object. Raw: {raw[:200]}")
                # fallback: use heuristic majority
                if non_manual_count > manual_count:
                    return "NON_MANUALS"
                else:
                    return "MANUAL"

            parsed = json.loads(m.group(0))
            info_type = (parsed.get('info_type', 'MANUAL') or 'MANUAL').strip().upper()

            # If model returns unknown value, fallback
            if info_type not in ("NON_MANUALS", "MANUAL"):
                logger.warning(f"LLM returned unexpected info_type: {info_type} - falling back to heuristic")
                return "NON_MANUALS" if non_manual_count > manual_count else "MANUAL"

            return info_type

        except Exception as e:
            logger.error(f"❌ Error in query classification (LLM path): {e}\n{traceback.format_exc()}")
            # Safe fallback
            return "NON_MANUALS" if non_manual_count > manual_count else "MANUAL"
   
    def validate_marine_query(self, query: str) -> bool:
        """Validate if query is related to marine equipment"""
        prompt = f"""
            Determine whether the following query is related to maritime or marine domain,
            including equipment, machinery, operations, maintenance, or troubleshooting.

            Query: "{query}"

            Guidelines:
            - Return True if the query refers to *any* equipment, system, or process that could reasonably 
            apply to marine engineering, ship machinery, propulsion, navigation, or vessel maintenance — 
            even if not explicitly mentioned (e.g., "Overhauling procedure", "Lube oil system issue").
            - Return True for general technical or operational terms that are *commonly* used in maritime contexts.
            - Return False only if it is clearly unrelated to marine or ship systems (e.g., "Fix my car engine", 
            "Laptop overheating", "Office internet not working").

            Respond strictly with True or False only.
            """
        try:
            response = self.generate_llama_response(prompt, temperature=0.1).strip().lower()
            return response == 'true'
        except Exception as e:
            logger.error(f"❌ Error in query validation: {str(e)}")
            return True  # Default to allowing the query
    
    def _deserialize_dynamodb_item(item):
        """Converts DynamoDB JSON (with M/L/S keys) into a native Python dict."""
        deserializer = TypeDeserializer()
        return deserializer.deserialize(item)

    # -----------------------------------------
    # Context-aware rephrase with numbering
    # -----------------------------------------
    def rephrase_query_with_context(self, query: str, chat_summary: dict, equipment_context: dict) -> str:
        """
        Rephrases the current question using full chat history context.
        Builds structured chronological context like:
        Question 1: ...
        Equipment: ...
        Then appends the current question snapshot.
        """
        try:
            # === 1️⃣ Build chronological Q&A context ===
            nav_flow_raw = chat_summary.get("navigation_flow", [])
            if isinstance(nav_flow_raw, dict) and "L" in nav_flow_raw:
                nav_flow = [self._deserialize_dynamodb_item(x) for x in nav_flow_raw["L"]]
            else:
                nav_flow = nav_flow_raw
            formatted_lines = []

            # Build formatted context from history_text if nav_flow is empty
            if isinstance(nav_flow, list) and nav_flow:
                for i, entry in enumerate(nav_flow):
                    q_text = entry.get("rephrased") or entry.get("question") or ""
                    per_eq = entry.get("equipment_context", {}) or {}
                    eq_str_parts = [f"{k.capitalize()}: {v}" for k, v in per_eq.items() if v and k in ("make","model","equipment")]
                    eq_str = "; ".join(eq_str_parts) if eq_str_parts else "No equipment info"
                    formatted_lines.append(f"Question {i+1}: {q_text}\nEquipment: {eq_str}")
            else:
                # 🧠 Fall back to using chat_summary['history_text'] instead of just current query
                hist_text = chat_summary.get("history_text", "")
                hist_text = hist_text.strip()
                if hist_text:
                    user_only_lines = []
                    for line in hist_text.splitlines():
                        line = line.strip()
                        if line.lower().startswith("user:"):
                            # remove the "User:" prefix and keep the clean text
                            user_only_lines.append(line.replace("User:", "").strip())

                    if user_only_lines:
                        clean_history = " ".join(user_only_lines)
                        formatted_lines.append(
                            f"Question 1: {clean_history}\nEquipment: {', '.join([f'{k}: {v}' for k, v in equipment_context.items() if v]) or 'No equipment info'}"
                        )
                else:
                    formatted_lines.append(f"Question 1: {query}\nEquipment: {', '.join([f'{k}: {v}' for k, v in equipment_context.items() if v]) or 'No equipment info'}")

            formatted_context = "\n\n".join(formatted_lines)

            # === 2️⃣ Build current question snapshot ===
            eq_parts = [
                f"{k.capitalize()}: {v}"
                for k, v in equipment_context.items()
                if v and k in ("make", "model", "equipment", "vessel")
            ]
            equipment_str = "; ".join(eq_parts) if eq_parts else "No equipment info for current question."

            # === 3️⃣ Build the final prompt ===
            prompt = f"""
    You are a maritime AI assistant that rephrases user questions for accurate retrieval and RAG.
    Treat the 'Question N:' items below as a chronological log (Question 1 is oldest). Do NOT merge distinct questions.
    Return a single line that starts with: Rephrased Query: <the rephrased question>.

    Previous conversation (chronological):
    {formatted_context}

    Current per-question equipment snapshot:
    {equipment_str}

    Current User Question:
    "{query}"

    Rules:
    - Respect the chronology above.
    - Rephrase only the current question.
    - Do NOT merge older topics.
    - Do NOT ask for missing details if already present.
    - Output one line starting with "Rephrased Query:".
    """
            # === 4️⃣ Send to Mistral ===
            response = self.generate_claude_response(prompt)

            # === 5️⃣ Clean up the output ===
            cleaned = response.strip()
            if cleaned.lower().startswith("rephrased query:"):
                cleaned = cleaned[len("rephrased query:"):].strip()
            cleaned = " ".join(cleaned.splitlines()).strip()  # collapse line breaks

            logger.info(f"🧠 Rephrased Query: {cleaned}")
            return cleaned

        except Exception as e:
            logger.error(f"❌ Error in query rephrasing: {str(e)}")
            return query

    
    def generate_rag_response(self, query: str, context: str, equipment_context: Dict[str, str]) -> str:
        """Generate RAG response with equipment awareness - FIXED to preserve S3 URLs"""
        equipment_info = ""
        
        if equipment_context.get('make') and equipment_context.get('model'):
            equipment_info = f"Specific Equipment: {equipment_context['make']} {equipment_context['model']}\n"
        
        if equipment_context.get('equipment'):
            equipment_info += f"Equipment Type: {equipment_context['equipment']}\n"
        
        prompt = f"""You are a Maritime Expert providing technical assistance. Answer the user's 
        question using the context provided, with attention to the specific equipment mentioned.

{equipment_info}
User Question: {query}

Context Information:
{context}

Instructions:
- Provide specific guidance relevant to identified equipment when possible
- Format response as structured markdown
- Include equipment-specific procedures and considerations
- Base answer strictly on provided context
- CRITICAL: When referencing sources, you MUST include the EXACT FULL SOURCE URL as provided in the context
- Do NOT modify or shorten source URLs - use them exactly as they appear (including s3:// paths)
- Include source references with page numbers in this exact format: Source: [EXACT_FULL_TEXT](EXACT_FULL_URL) (Page: [NUMBER])
- Example: Source: [s3://synergy-oe-propulsionpro-prod/source_data/pdf/oem/HIMSEN.PDF](https://synergy-oe-propulsionpro-prod/source_data/pdf/oem/HIMSEN.PDF) (Page: 123)
- If equipment-specific info unavailable, provide general guidance with note
- Ask if user wants more specific information about their equipment

MANDATORY: Every source reference MUST include the complete s3:// URL exactly as provided in the context.

Provide a comprehensive, equipment-aware response:"""
        
        try:
            rag_response = self.generate_claude_response(prompt)
            logger.info("✅ RAG response generated successfully")
            logger.info(f"🔍 RAG_RESPONSE_DEBUG: Length={len(rag_response)}")
            logger.info(f"🔍 RAG_RESPONSE_PREVIEW: {rag_response[:500]}...")
            
            # Check if response contains S3 URLs
            s3_count = rag_response.count('s3://')
            logger.info(f"🔍 RAG_RESPONSE_S3_COUNT: Found {s3_count} s3:// URLs in LLM response")
            
            return rag_response
            
        except Exception as e:
            logger.error(f"❌ Error generating RAG response: {str(e)}")
            return "I apologize, but I encountered an error generating the response. Please try again."
    
    def format_response_as_html(self, markdown_content: Union[str, Dict], user_query: str = "", 
                               missing_entities: List[str] = None,
                               url_mapping: Dict[str, str] = None) -> Dict[str, Any]:
        """
        PRODUCTION-READY: Format markdown content as HTML response with robust error handling and signed URL replacement
        
        Args:
            markdown_content: Input content (str, dict, or other types)
            user_query: User query for context
            missing_entities: List of missing entities to add to response
            url_mapping: Dictionary mapping S3 URLs to signed URLs
            
        Returns:
            Dict with standardized response format
        """
        logger.debug("🎨 Starting HTML formatting process with signed URL support...")
        
        try:
            # =====================================================================
            # STEP 1: INPUT VALIDATION AND TYPE HANDLING
            # =====================================================================
            processed_content = self._validate_and_process_input(markdown_content)
            logger.debug(f"📝 Processed content length: {len(str(processed_content))}")
            
            # =====================================================================
            # STEP 2: ADD MISSING ENTITY NOTICE
            # =====================================================================
            final_content = self._add_missing_entity_notice(processed_content, missing_entities)
            
            # =====================================================================
            # STEP 3: CHECK IF CONTENT IS ALREADY IN CORRECT FORMAT
            # =====================================================================
            if isinstance(markdown_content, dict) and self._is_valid_response_format(markdown_content):
                logger.debug("✅ Input already in correct response format")
                response = self._ensure_response_format(markdown_content)
            else:
                # =====================================================================
                # STEP 4: ATTEMPT LLM HTML CONVERSION (WITH FALLBACKS)
                # =====================================================================
                html_result = self._attempt_llm_html_conversion(final_content)
                
                if html_result:
                    logger.info("✅ HTML formatting completed successfully via LLM")
                    response = html_result
                else:
                    # =====================================================================
                    # STEP 5: FALLBACK TO SIMPLE HTML CONVERSION
                    # =====================================================================
                    logger.warning("⚠️ LLM HTML conversion failed, using fallback")
                    response = self._fallback_html_conversion(final_content)
            
            # =====================================================================
            # STEP 6: REPLACE S3 URLs WITH SIGNED URLs
            # =====================================================================
            if url_mapping and response.get('response'):
                original_length = len(response['response'])
                response['response'] = self._replace_s3_urls_with_signed(
                    response['response'], url_mapping
                )
                new_length = len(response['response'])
                logger.info(f"🔗 Replaced {len(url_mapping)} S3 URLs with signed URLs in response")
                logger.debug(f"📏 Content length change: {original_length} → {new_length}")
            
            logger.info("✅ HTML formatting with signed URL replacement completed successfully")
            return response
            
        except Exception as e:
            logger.error(f"❌ Critical error in HTML formatting: {str(e)}")
            # ULTIMATE FALLBACK
            return self._create_emergency_fallback_response(markdown_content, str(e))
        
    def _create_friendly_document_name(self, s3_url: str, page_mapping: Dict[str, str] = None) -> str:
        """Create user-friendly document name from S3 URL"""
        try:
            # Extract filename from S3 URL
            file_name = s3_url.split('/')[-1] if '/' in s3_url else s3_url
            
            # Remove file extension
            doc_name = file_name.rsplit('.', 1)[0] if '.' in file_name else file_name
            
            # Clean up common patterns in maritime document names
            doc_name = self._clean_document_name(doc_name)
            
            # Add page information if available
            if page_mapping and s3_url in page_mapping:
                page_num = page_mapping[s3_url]
                return f"{doc_name} - Page {page_num}"
            
            return f"{doc_name} Manual"
        
        except Exception as e:
            logger.error(f"❌ Error creating friendly document name: {str(e)}")
            # Fallback to filename
            return s3_url.split('/')[-1] if '/' in s3_url else s3_url
        
    def _clean_document_name(self, doc_name: str) -> str:
        """Clean up document name for better readability"""
        try:
            # Remove common prefixes/suffixes
            doc_name = re.sub(r'^(oem_|manual_|doc_)', '', doc_name, flags=re.IGNORECASE)
            doc_name = re.sub(r'(_manual|_doc|_instruction)$', '', doc_name, flags=re.IGNORECASE)
            
            # Replace underscores and hyphens with spaces
            doc_name = doc_name.replace('_', ' ').replace('-', ' ')
            
            # Convert to title case but preserve known abbreviations
            maritime_abbreviations = ['MC', 'MAN', 'B&W', 'IMO', 'SOLAS', 'MARPOL', 'FMECA']
            words = doc_name.split()
            
            cleaned_words = []
            for word in words:
                if word.upper() in maritime_abbreviations:
                    cleaned_words.append(word.upper())
                elif len(word) <= 3 and word.isalpha():  # Likely abbreviation
                    cleaned_words.append(word.upper())
                else:
                    cleaned_words.append(word.capitalize())
            
            result = ' '.join(cleaned_words)
            
            # Handle special maritime engine naming patterns
            result = re.sub(r'\b(S\d+MC)\b', r'\1', result)  # S50MC, S60MC, etc.
            result = re.sub(r'\b(RT[\s-]?flex\d+C?)\b', r'RT-flex\1', result, flags=re.IGNORECASE)
            
            return result.strip()
            
        except Exception as e:
            logger.error(f"❌ Error cleaning document name: {str(e)}")
            return doc_name
        
    def _extract_page_info_from_context(self, content: str, position: int) -> str:
        """Extract page information from surrounding context"""
        try:
            # Look for page information in nearby text (within 200 characters)
            start = max(0, position - 200)
            end = min(len(content), position + 200)
            context = content[start:end]
            
            # Look for patterns like "(Page: 27)" or "Page 27"
            page_patterns = [
                r'\(Page:\s*(\d+)\)',
                r'Page\s*(\d+)',
                r'page\s*(\d+)',
                r'p\.?\s*(\d+)'
            ]
            
            for pattern in page_patterns:
                match = re.search(pattern, context, re.IGNORECASE)
                if match:
                    return match.group(1)
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error extracting page info from context: {str(e)}")
            return None
    
    def _replace_s3_urls_with_signed(self, content: str, url_mapping: Dict[str, str], 
                                page_mapping: Dict[str, str] = None) -> str:
        """Replace S3 URLs in content with signed URLs using document names and page numbers"""
        try:
            modified_content = content
            replacements_made = 0
            
            # Debug logging
            logger.info(f"🔍 URL replacement debug - Content length: {len(content)}")
            logger.info(f"🔍 URL mapping has {len(url_mapping)} URLs")
            
            for s3_url, signed_url in url_mapping.items():
                logger.info(f"🔍 Processing S3 URL: {s3_url}")
                
                # Extract document name and create user-friendly name
                friendly_name = self._create_friendly_document_name(s3_url, page_mapping)
                
                # Count occurrences before replacement
                original_count = modified_content.count(s3_url)
                logger.info(f"🔍 Exact matches found: {original_count}")
                
                if original_count > 0:
                    # Strategy 1: Replace full S3 URLs with friendly named links
                    modified_content = modified_content.replace(
                        s3_url, 
                        f'<a href="{signed_url}" target="_blank">{friendly_name}</a>'
                    )
                    
                    # Strategy 2: Replace in href attributes while keeping friendly names
                    modified_content = re.sub(
                        rf'href="{re.escape(s3_url)}"([^>]*>)([^<]*)</a>',
                        rf'href="{signed_url}"\1{friendly_name}</a>',
                        modified_content
                    )
                    
                    replacements_made += original_count
                    logger.info(f"🔗 Replaced {original_count} exact matches with friendly name: {friendly_name}")
                    
                else:
                    # Strategy 3: Look for Source: patterns and replace with friendly names
                    source_pattern = rf'Source:\s*{re.escape(s3_url)}\s*\(Page:\s*(\d+)\)'
                    matches = re.finditer(source_pattern, modified_content)
                    
                    for match in matches:
                        page_num = match.group(1)
                        friendly_name_with_page = self._create_friendly_document_name(s3_url, {s3_url: page_num})
                        
                        modified_content = modified_content.replace(
                            match.group(0),
                            f'Source: <a href="{signed_url}" target="_blank">{friendly_name_with_page}</a>'
                        )
                        replacements_made += 1
                        logger.info(f"🔗 Replaced source reference with: {friendly_name_with_page}")
                    
                    # Strategy 4: Look for reference links like [1], [2] and enhance them
                    ref_pattern = r'<a href="[^"]*" target="_blank">\[(\d+)\]</a>'
                    ref_matches = re.finditer(ref_pattern, modified_content)
                    
                    for ref_match in ref_matches:
                        ref_num = ref_match.group(1)
                        # Find corresponding page info from nearby text
                        page_info = self._extract_page_info_from_context(modified_content, ref_match.start())
                        
                        if page_info:
                            enhanced_name = f"{friendly_name} - Page {page_info}"
                        else:
                            enhanced_name = f"{friendly_name} - Ref [{ref_num}]"
                        
                        modified_content = modified_content.replace(
                            ref_match.group(0),
                            f'<a href="{signed_url}" target="_blank">{enhanced_name}</a>'
                        )
                        replacements_made += 1
                        logger.info(f"🔗 Enhanced reference link: {enhanced_name}")
            
            # Strategy 5: If no replacements made, add friendly named links section
            if replacements_made == 0 and url_mapping:
                logger.info("🔍 No URL replacements made, adding friendly named links section")
                signed_urls_section = "\n\n<h3>Document Sources:</h3>\n<ul>\n"
                
                for s3_url, signed_url in url_mapping.items():
                    friendly_name = self._create_friendly_document_name(s3_url, page_mapping)
                    signed_urls_section += f'<li><a href="{signed_url}" target="_blank">{friendly_name}</a></li>\n'
                
                signed_urls_section += "</ul>"
                
                # Insert before closing body tag or append
                if '</body>' in modified_content:
                    modified_content = modified_content.replace('</body>', f'{signed_urls_section}</body>')
                else:
                    modified_content += signed_urls_section
                
                replacements_made = len(url_mapping)
                logger.info(f"🔗 Added {len(url_mapping)} friendly named links in dedicated section")
            
            logger.info(f"✅ URL replacement completed: {replacements_made} total replacements made")
            return modified_content
            
        except Exception as e:
            logger.error(f"❌ Error replacing S3 URLs with signed URLs: {str(e)}")
            return content  # Return original content if replacement fails


    def _validate_and_process_input(self, content: Union[str, Dict, Any]) -> str:
        """Validate and process input content to string format"""
        try:
            if content is None:
                return "No content provided."
            
            # Handle dictionary input
            if isinstance(content, dict):
                # If it looks like a response format, extract the response text
                if 'response' in content:
                    return str(content['response'])
                # If it has a text-like field
                elif 'text' in content:
                    return str(content['text'])
                # If it has content field
                elif 'content' in content:
                    return str(content['content'])
                # Otherwise convert entire dict to string
                else:
                    return json.dumps(content, indent=2)
            
            # Handle list input
            elif isinstance(content, list):
                return '\n'.join(str(item) for item in content)
            
            # Handle other types
            elif isinstance(content, (int, float, bool)):
                return str(content)
            
            # Handle bytes
            elif isinstance(content, bytes):
                return content.decode('utf-8', errors='replace')
            
            # Handle string (most common case)
            else:
                return str(content)
                
        except Exception as e:
            logger.error(f"❌ Error processing input content: {str(e)}")
            return f"Error processing content: {str(e)}"
    
    def _add_missing_entity_notice(self, content: str, missing_entities: List[str] = None) -> str:
        """Add missing entity notice to content"""
        if not missing_entities:
            return content
        
        try:
            missing_vars = ", ".join(missing_entities)
            entity_notice = f"\n\n**Note**: To provide better results, please specify the {missing_vars}."
            return content + entity_notice
        except Exception as e:
            logger.error(f"❌ Error adding entity notice: {str(e)}")
            return content
    
    def _is_valid_response_format(self, data: Dict) -> bool:
        """Check if data is already in valid response format"""
        try:
            required_fields = ['response', 'response_type', 'Data_json']
            return all(field in data for field in required_fields)
        except Exception:
            return False
    
    def _ensure_response_format(self, data: Dict) -> Dict[str, Any]:
        """Ensure data has all required response format fields"""
        try:
            return {
                "response": data.get('response', ''),
                "response_type": data.get('response_type', 'HTML'),
                "Data_json": data.get('Data_json', [])
            }
        except Exception as e:
            logger.error(f"❌ Error ensuring response format: {str(e)}")
            return self._create_standard_response(str(data), "Text")
    
    def _attempt_llm_html_conversion(self, content: str) -> Optional[Dict[str, Any]]:
        """Attempt to convert content to HTML using LLM with robust error handling"""
        try:
            # Limit content length to prevent token overflow
            max_length = 8000  # Conservative limit
            if len(content) > max_length:
                logger.warning(f"⚠️ Content too long ({len(content)} chars), truncating to {max_length}")
                content = content[:max_length] + "... [Content truncated]"
            
            prompt = f"""Convert the following content to clean HTML format and return ONLY a valid JSON response.

Content to convert:
{content}

Requirements:
1. Convert markdown/text to proper HTML
2. Ensure all links have target="_blank"
3. Use proper HTML formatting (paragraphs, lists, headers, etc.)
4. Return ONLY this exact JSON structure:

{{
    "response": "HTML_CONTENT_HERE",
    "response_type": "HTML", 
    "Data_json": []
}}

Important: Return ONLY the JSON object, no other text or explanation."""

            # Get LLM response
            llm_response = self.generate_llama_response(prompt, temperature=0.1)
            
            if not llm_response:
                logger.warning("⚠️ Empty response from LLM")
                return None
            
            # Parse the response with multiple fallback methods
            return self._parse_llm_html_response(llm_response, content)
            
        except Exception as e:
            logger.error(f"❌ Error in LLM HTML conversion: {str(e)}")
            return None
    
    def _parse_llm_html_response(self, llm_response: str, original_content: str) -> Optional[Dict[str, Any]]:
        """Parse LLM response with multiple fallback methods"""
        try:
            # Method 1: Direct JSON parsing
            try:
                parsed = json.loads(llm_response.strip())
                if self._validate_parsed_response(parsed):
                    logger.debug("✅ Method 1: Direct JSON parsing successful")
                    return parsed
            except json.JSONDecodeError:
                pass
            
            # Method 2: Clean and parse JSON
            try:
                cleaned = self._clean_json_response(llm_response)
                parsed = json.loads(cleaned)
                if self._validate_parsed_response(parsed):
                    logger.debug("✅ Method 2: Cleaned JSON parsing successful")
                    return parsed
            except json.JSONDecodeError:
                pass
            
            # Method 3: Extract JSON with regex
            try:
                json_pattern = r'\{[^{}]*"response"[^{}]*"response_type"[^{}]*"Data_json"[^{}]*\}'
                json_match = re.search(json_pattern, llm_response, re.DOTALL)
                if json_match:
                    parsed = json.loads(json_match.group())
                    if self._validate_parsed_response(parsed):
                        logger.debug("✅ Method 3: Regex JSON extraction successful")
                        return parsed
            except (json.JSONDecodeError, AttributeError):
                pass
            
            # Method 4: Try to extract just the HTML content
            try:
                html_content = self._extract_html_content(llm_response)
                if html_content:
                    logger.debug("✅ Method 4: HTML content extraction successful")
                    return self._create_standard_response(html_content, "HTML")
            except Exception:
                pass
            
            logger.warning("⚠️ All parsing methods failed")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error parsing LLM response: {str(e)}")
            return None
    
    def _validate_parsed_response(self, parsed: Dict) -> bool:
        """Validate that parsed response has correct structure"""
        try:
            if not isinstance(parsed, dict):
                return False
            
            # Check required fields
            if 'response' not in parsed:
                return False
            
            # Ensure response_type is valid
            response_type = parsed.get('response_type', 'HTML')
            if response_type not in ['HTML', 'Text', 'Error']:
                parsed['response_type'] = 'HTML'
            
            # Ensure Data_json is a list
            if 'Data_json' not in parsed:
                parsed['Data_json'] = []
            elif not isinstance(parsed['Data_json'], list):
                parsed['Data_json'] = []
            
            return True
            
        except Exception:
            return False
    
    def _extract_html_content(self, response: str) -> Optional[str]:
        """Try to extract HTML content from LLM response"""
        try:
            # Look for content between quotes that looks like HTML
            html_patterns = [
                r'"response"\s*:\s*"([^"]*(?:\\.[^"]*)*)"',
                r'<[^>]+>.*?</[^>]+>',
                r'<p>.*?</p>',
                r'<div>.*?</div>'
            ]
            
            for pattern in html_patterns:
                match = re.search(pattern, response, re.DOTALL)
                if match:
                    content = match.group(1) if len(match.groups()) > 0 else match.group(0)
                    # Unescape JSON string if needed
                    if '\\n' in content or '\\"' in content:
                        content = content.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')
                    return content
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error extracting HTML content: {str(e)}")
            return None
    
    def _fallback_html_conversion(self, content: str) -> Dict[str, Any]:
        """Fallback method to convert content to HTML without LLM"""
        try:
            # Simple markdown-to-HTML conversion
            html_content = self._simple_markdown_to_html(content)
            return self._create_standard_response(html_content, "HTML")
            
        except Exception as e:
            logger.error(f"❌ Error in fallback HTML conversion: {str(e)}")
            # Ultimate fallback - return as text
            return self._create_standard_response(content, "Text")
    
    def _simple_markdown_to_html(self, content: str) -> str:
        """Simple markdown to HTML conversion without external dependencies"""
        try:
            html = content
            
            # Headers
            html = re.sub(r'^### (.*?)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
            html = re.sub(r'^## (.*?)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
            html = re.sub(r'^# (.*?)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)
            
            # Bold and italic
            html = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', html)
            html = re.sub(r'\*(.*?)\*', r'<em>\1</em>', html)
            
            # Links
            html = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2" target="_blank">\1</a>', html)
            
            # Line breaks and paragraphs
            html = html.replace('\n\n', '</p><p>')
            html = html.replace('\n', '<br>')
            html = f'<p>{html}</p>'
            
            # Clean up empty paragraphs
            html = re.sub(r'<p></p>', '', html)
            html = re.sub(r'<p>\s*</p>', '', html)
            
            return html
            
        except Exception as e:
            logger.error(f"❌ Error in simple markdown conversion: {str(e)}")
            return content  # Return original if conversion fails
    
    def _create_standard_response(self, content: str, response_type: str = "HTML") -> Dict[str, Any]:
        """Create standardized response format"""
        return {
            "response": str(content),
            "response_type": response_type,
            "Data_json": []
        }
    
    def _create_emergency_fallback_response(self, original_content: Any, error_msg: str) -> Dict[str, Any]:
        """Create emergency fallback response when everything fails"""
        try:
            # Try to get some usable content
            if isinstance(original_content, dict) and 'response' in original_content:
                content = str(original_content['response'])
            else:
                content = str(original_content)
            
            return {
                "response": content,
                "response_type": "Text",
                "Data_json": []
            }
            
        except Exception:
            # Absolute final fallback
            return {
                "response": "An error occurred while formatting the response. Please try again.",
                "response_type": "Error",
                "Data_json": []
            }


# Global LLM client instance - Create it at module level to avoid circular imports
def get_llm_client():
    """Factory function to get LLM client instance"""
    if not hasattr(get_llm_client, '_instance'):
        get_llm_client._instance = LLMClient()
    return get_llm_client._instance

# Create global instance
llm_client = get_llm_client()