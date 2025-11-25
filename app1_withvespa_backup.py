"""
Maritime RAG Application Main Orchestrator - CLEAN SESSION MANAGEMENT VERSION
+ Streaming logic integrated (from prod) - minimal changes, streaming only.
Implements the complete flow with proper session management and flowchart-compliant entity handling
Focus on core functionality with clear debugging prints
"""

import os
import json
import uuid
import datetime
import logging
import traceback
import boto3
import botocore
import re
import pandas as pd
import time
import threading
from typing import Dict, List, Optional, Tuple, Any
from spellchecker import SpellChecker

# Import all modules (CLEAN IMPORTS)
from config import config
from llm_client import llm_client
from dynamodb import dynamo_manager
from vespa_client import vespa_client
from entity_extraction import entity_extractor
from signed_url_generator import create_signed_url_mapping, get_signed_url_stats

# AWS / model constants for streaming (adjust as needed)
AWS_REGION = "ap-south-1"
MODEL_ID = "apac.anthropic.claude-sonnet-4-20250514-v1:0"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# WEBSOCKET SERVICE
# =============================================================================

class WebSocketService:
    """Handles WebSocket communication"""
    
    def __init__(self):
        self.aws_credentials = config.get_aws_credentials()
        self.ws_endpoint = config.ws_endpoint
        self.ws_client = None
        self._initialize_websocket_client()
    
    def _initialize_websocket_client(self):
        """Initialize WebSocket API Gateway client"""
        logger.error(f"🔎 WS endpoint config: {self.ws_endpoint!r}")
        if not self.ws_endpoint:
            logger.warning("⚠️ WebSocket endpoint not configured")
            return
        
        try:
            endpoint_url = self.ws_endpoint
            if endpoint_url.startswith('wss://'):
                endpoint_url = 'https://' + endpoint_url[6:]
            
            self.ws_client = boto3.client(
                'apigatewaymanagementapi',
                endpoint_url=endpoint_url,
                **self.aws_credentials
            )
            logger.info("✅ WebSocket client initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize WebSocket client: {str(e)}")
            self.ws_client = None
    
    def send_message(self, connection_id: str, message: str) -> bool:
        """Send message via WebSocket"""
        try:
            # Handle test mode - robust check for different config formats
            test_mode = False
            try:
                if hasattr(config, 'is_test_mode'):
                    is_test_mode_attr = getattr(config, 'is_test_mode')
                    if callable(is_test_mode_attr):
                        test_mode = is_test_mode_attr()
                    else:
                        test_mode = bool(is_test_mode_attr)
                elif hasattr(config, 'TEST_MODE'):
                    test_mode = bool(getattr(config, 'TEST_MODE'))
                else:
                    test_mode = connection_id == "test-connection-123"
            except Exception as e:
                logger.warning(f"⚠️ Error checking test mode: {str(e)}, defaulting to test mode for test connections")
                test_mode = connection_id == "test-connection-123"
            
            if connection_id == "test-connection-123" and test_mode:
                logger.info(f"📤 Test mode: Would send to {connection_id}")
                return True
            
            if not self.ws_client:
                logger.error("❌ WebSocket client not configured")
                return False
            
            self.ws_client.post_to_connection(
                Data=message.encode('utf-8') if isinstance(message, str) else message,
                ConnectionId=connection_id
            )
            
            logger.info(f"📤 Message sent to connection {connection_id}")
            return True
            
        except botocore.exceptions.ClientError as e:
            err_code = None
            err_msg = str(e)
            try:
                err_code = e.response.get("Error", {}).get("Code")
                err_msg = e.response.get("Error", {}).get("Message", err_msg)
            except Exception:
                pass

            logger.error(f"❌ WebSocket client error: code={err_code} message={err_msg} connection_id={connection_id}")
            if err_code == "GoneException":
                logger.warning(f"⚠️ Connection {connection_id} is no longer valid (GoneException)")
            else:
                logger.error(f"❌ WebSocket error details: {e}")
            return False

            
        except Exception as e:
            logger.error(f"❌ Error sending WebSocket message: {str(e)}")
            return False
        
# ==========================
# SERVER-SIDE CONNECTION MANAGER
# ==========================
class WebSocketConnectionManager:
    """Server-side WebSocket connection management - HEARTBEAT ONLY approach."""
    def __init__(self, websocket_service):
        self.websocket_service = websocket_service
        self.active_connections = {}
        self.heartbeat_interval = 20
        self.heartbeat_threads = {}
        # heartbeat rate-limiting tracker
        self.last_heartbeat_time = {}

    def register_processing_connection(self, connection_id: str, topic_id: str, transaction_id: str):
        self.active_connections[connection_id] = {
            'topic_id': topic_id,
            'transaction_id': transaction_id,
            'last_heartbeat': time.time(),
            'start_time': time.time(),
            'status': 'processing',
            'heartbeat_active': False
        }
        self.last_heartbeat_time[connection_id] = 0
        logger.info(f"📝 Registered connection {connection_id} for processing")

    def start_heartbeat_for_connection(self, connection_id: str):
        if connection_id not in self.active_connections:
            return False
        if self.active_connections[connection_id]['heartbeat_active']:
            logger.info(f"💓 Heartbeat already active for {connection_id}")
            return True

        # Send one immediate heartbeat
        if not self.send_heartbeat(connection_id, "Connection established - starting maritime processing"):
            logger.warning(f"⚠️ Initial heartbeat failed for {connection_id}")
            return False

        def heartbeat_worker():
            self.active_connections[connection_id]['heartbeat_active'] = True
            heartbeat_count = 0
            while connection_id in self.active_connections:
                try:
                    time.sleep(self.heartbeat_interval)
                    if connection_id not in self.active_connections:
                        break
                    heartbeat_count += 1
                    elapsed = int(time.time() - self.active_connections[connection_id]['start_time'])
                    heartbeat_msg = f"Scheduled heartbeat #{heartbeat_count} - Maritime processing active ({elapsed}s elapsed)"
                    success = self.send_heartbeat(connection_id, heartbeat_msg)
                    if not success:
                        logger.warning(f"💔 Scheduled heartbeat #{heartbeat_count} failed for {connection_id} - stopping worker")
                        break
                except Exception as e:
                    logger.error(f"❌ Heartbeat worker error for {connection_id}: {e}")
                    break
            if connection_id in self.heartbeat_threads:
                del self.heartbeat_threads[connection_id]
            if connection_id in self.active_connections:
                self.active_connections[connection_id]['heartbeat_active'] = False
            logger.info(f"💓 Heartbeat worker terminated for {connection_id} (sent {heartbeat_count} beats)")

        th = threading.Thread(target=heartbeat_worker, daemon=True, name=f"Heartbeat-{connection_id}")
        th.start()
        self.heartbeat_threads[connection_id] = th
        logger.info(f"💓 Heartbeat thread started for {connection_id}")
        return True

    def can_send_heartbeat(self, connection_id: str) -> bool:
        if connection_id not in self.last_heartbeat_time:
            return True
        time_since_last = time.time() - self.last_heartbeat_time[connection_id]
        return time_since_last >= 15

    def send_heartbeat(self, connection_id: str, custom_message: str = None) -> bool:
        try:
            if connection_id not in self.active_connections:
                logger.warning(f"⚠️ Cannot send heartbeat - connection {connection_id} not registered")
                return False
            processing_time = int(time.time() - self.active_connections[connection_id]['start_time'])
            heartbeat_message = {
                "type": "heartbeat",
                "connection_id": connection_id,
                "message": custom_message or "Connection alive",
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "processing_time": processing_time,
                "statusCode": 102,
                "body": "Processing"
            }
            success = self.websocket_service.send_message(connection_id, json.dumps(heartbeat_message))
            if success:
                self.active_connections[connection_id]['last_heartbeat'] = time.time()
                self.last_heartbeat_time[connection_id] = time.time()
            return success
        except Exception as e:
            logger.error(f"❌ Error sending heartbeat to {connection_id}: {e}")
            return False

    def remove_connection(self, connection_id: str):
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]
        if connection_id in self.last_heartbeat_time:
            del self.last_heartbeat_time[connection_id]
        logger.info(f"🗑️ Removed connection {connection_id} from tracking")

    def is_connection_active(self, connection_id: str) -> bool:
        return connection_id in self.active_connections
    

class ProcessingStatusTracker:
    """Tracks and stores processing status for long-running operations (lightweight)."""
    @staticmethod
    def save_processing_status(topic_id: str, status: str, progress: int = 0, details: Optional[str] = None):
        try:
            status_data = {
                "topic_id": topic_id,
                "status": status,
                "progress": progress,
                "details": details or "",
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ttl": int(time.time()) + 7200
            }
            logger.info(f"💾 Saved processing status: {status} ({progress}%) for topic {topic_id}")
            # Optional: dynamo_manager.save_processing_status(status_data) if you implement
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save processing status: {e}")
            return False



# =============================================================================
# TOPIC NAME GENERATOR
# =============================================================================

class TopicNameGenerator:
    """Handles topic name generation using LLM"""

    @staticmethod
    def generate_topic_name(user_prompt: str) -> str:
        try:
            print("🏷️ TOPIC_GENERATION: Generating topic name for query")

            # Stronger guardrail prompt
            topic_prompt = f"""
            Generate a short, descriptive topic name (max 5–6 words) for the following marine equipment query. 
            The topic must be concise, grammatically correct, and free of spelling mistakes. 
            Always use proper capitalization for marine brands (e.g., Wärtsilä, Yanmar, Caterpillar, MAN B&W).

            User Query: {user_prompt}

            Response format: Just return the topic name as plain text, nothing else.
            Examples:
            - "Engine Troubleshooting"
            - "Pump Maintenance Guide"
            - "Electrical System Issues"
            - "Safety Equipment Inspection"
            """

            # Call LLM
            topic_name = llm_client.generate_mistral_response(topic_prompt)
            topic_name = topic_name.strip().strip('"').strip("'")

            # --- Post-process step 1: spell-check ---
            spell = SpellChecker()
            count = len(list(spell.word_frequency.words()))
            logger.warning(f"⚠️ Number of words in dictionary: {count}")
            marine_terms = {"Wärtsilä", "Yanmar", "Caterpillar", "MAN", "B&W", "Marine", "Propulsion"}  

            def normalize_word(word: str) -> str:
                # Collapse double leading chars like "MMarine" → "Marine"
                return re.sub(r'^([A-Za-z])\1+', r'\1', word)

            corrected_words = []
            for word in topic_name.split():
                normalized = normalize_word(word)

                if normalized in marine_terms:
                    corrected_words.append(normalized.title())
                elif normalized.lower() in spell:  # valid word
                    corrected_words.append(normalized.title())
                else:
                    correction = spell.correction(normalized)
                    corrected_words.append(correction.title() if correction else normalized)


            topic_name = " ".join(corrected_words)

            # --- Post-process step 2: enforce length ---
            if len(topic_name) > 50:
                topic_name = topic_name[:47] + "..."

            if not topic_name or len(topic_name.split()) < 2:
                logger.warning(f"⚠️ Bad topic_name generated, falling back")
                topic_name = "Marine Equipment Query"

            print(f"✅ TOPIC_GENERATED: {topic_name}")
            return topic_name

        except Exception as e:
            logger.error(f"❌ Error generating topic name: {str(e)}")
            return "Marine Equipment Query"


# =============================================================================
# RESPONSE FORMATTER
# =============================================================================

class ResponseFormatter:
    """Handles response formatting and validation"""
    
    @staticmethod
    def create_standard_response(response_text: str, response_type: str = "Text", 
                               data_json: Optional[List] = None) -> Dict[str, Any]:
        """Create standardized response format"""
        if data_json is None:
            data_json = []
            
        return {
            "response": response_text,
            "response_type": response_type,
            "Data_json": data_json
        }
    
    @staticmethod
    def validate_response_format(response: Dict[str, Any]) -> bool:
        """Validate response follows required format"""
        required_fields = ['response', 'response_type', 'Data_json']
        
        for field in required_fields:
            if field not in response:
                logger.warning(f"⚠️ Missing required field: {field}")
                return False
        
        return True

# =============================================================================
# MAIN APPLICATION CLASS
# =============================================================================

class MaritimeRAGApplication:
    """Main application orchestrator with flowchart-compliant entity handling + streaming"""
    
    def __init__(self):
        """Initialize all application components"""
        logger.info("🚀 Initializing Maritime RAG Application (UAT) with streaming logic integrated...")
        
        # Initialize services
        self.websocket_service = WebSocketService()
        self.connection_manager = WebSocketConnectionManager(self.websocket_service)
        self.topic_generator = TopicNameGenerator()
        self.status_tracker = ProcessingStatusTracker()
        self.response_formatter = ResponseFormatter()
        
        # small startup log so you can confirm new code is running in UAT logs
        logger.info("🔥 STREAMING LOGIC: Integrated and ready (UAT app.py)")
        
        logger.info("✅ Maritime RAG Application initialized successfully")
    
    # -------------------------------------------------------------------------
    # Session Management
    # -------------------------------------------------------------------------
    
    def is_new_session(self, topic_id: str) -> bool:
        """Determine if this is a new session"""
        if not topic_id:
            return True
        
        try:
            chat_history = dynamo_manager.get_chat_history(topic_id)
            message_count = len(chat_history) if chat_history else 0
            print(f"📊 SESSION_CHECK: Topic {topic_id} has {message_count} existing messages")
            return message_count == 0
        except Exception as e:
            logger.error(f"❌ Error checking session status: {str(e)}")
            return True
    
    # -------------------------------------------------------------------------
    # STREAMING SUPPORT (ADDED)
    # -------------------------------------------------------------------------
    
    @staticmethod
    def create_bedrock_client():
        """Creates and returns a boto3 Bedrock Runtime client using environment variables."""
        aws_access_key = os.getenv("prod_aws_access_key_id")
        aws_secret_key = os.getenv("prod_aws_secret_access_key")
        
        if not all([aws_access_key, aws_secret_key]):
            raise EnvironmentError("Missing AWS credentials in environment variables for Bedrock client.")
        
        return boto3.client(
            "bedrock-runtime",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=AWS_REGION
        )
    
    def handle_connection_loss(self, topic_id: str, transaction_id: str, response_data: Dict[str, Any], topic_name: Optional[str] = None):
        """Store final response when connection is lost for reconnection retrieval."""
        try:
            final_response_record = {
                "topic_id": topic_id,
                "transaction_id": transaction_id,
                "final_response": response_data,
                "topic_name": topic_name,
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "status": "completed_disconnected",
                "ttl": int(time.time()) + 3600
            }
            logger.info(f"💾 Stored final response for disconnected session: {topic_id}")
            if hasattr(dynamo_manager, "store_final_response"):
                dynamo_manager.store_final_response(final_response_record)
        except Exception as e:
            logger.error(f"❌ Failed to store final response: {e}")

    
    def _send_ws_chunk(self, connection_id: str, text: str, retries: int = 3,
                    transaction_id: str = "streaming-partial",
                    user_prompt: str = "",
                    topic_id: str = "",
                    prompt_timestamp: str = "",
                    topic_name: str = "") -> bool:
        """Helper to send a streaming chunk with unified format."""
        response_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        message = {
            "transaction_id": transaction_id,
            "Prompt": user_prompt,
            "TopicID": topic_id,
            "TopicName": topic_name,
            "Prompt_timestamp": prompt_timestamp,
            "Response_timestamp": response_timestamp,
            "Response": {
                "response": text,
                "response_type": "HTML_STREAM",
                "Data_json": [],
                "is_final": False
            },
            "statusCode": 102,
            "body": "Processing",
        }

        payload = json.dumps(message)
        if len(payload.encode("utf-8")) > 128 * 1024:
            logger.error("Payload too large for WebSocket message")
            return False

        for attempt in range(retries):
            if self.websocket_service.send_message(connection_id, payload):
                return True
            time.sleep(0.2 * (attempt + 1))
        return False


    def attempt_final_response_delivery(self, connection_id: str, transaction_id: str,
                                   user_prompt: str, topic_id: str, prompt_timestamp: str,
                                   response_data: Dict[str, Any], topic_name: Optional[str] = None,
                                   url_mapping: Optional[dict] = None) -> bool:
        try:
            logger.info(f"🎯 FINAL_DELIVERY_ATTEMPT: Trying to send final response to {connection_id}")
            success = self.send_websocket_response(
                connection_id, transaction_id, user_prompt, topic_id,
                prompt_timestamp, response_data, topic_name=topic_name, url_mapping=url_mapping
            )
            if success:
                logger.info("✅ Final response delivered successfully")
                return True
            else:
                logger.warning("❌ Final response delivery failed - storing for later retrieval")
                self.handle_connection_loss(topic_id, transaction_id, response_data, topic_name)
                return False
        except Exception as e:
            logger.error(f"❌ Error in final delivery attempt: {str(e)}")
            self.handle_connection_loss(topic_id, transaction_id, response_data, topic_name)
            return False


    def stream_llm_response(self, connection_id: str, transaction_id: str,
        user_prompt: str, topic_id: str, prompt_timestamp: str,
        rephrased_query: str, context: str, equipment_context: dict,
        yql: str, topic_name: Optional[str] = None, url_mapping: Optional[dict] = None,
        max_chunk_interval: float = 0.001, max_chunk_size: int = 1) -> bool:

        if url_mapping is None:
            url_mapping = {}

        """
        Stream LLM tokens to the frontend WebSocket, with buffering and simple retries.
        Falls back to single-shot if streaming fails.
        """
        try:
            bedrock_client = self.create_bedrock_client()

            system_prompt = """You are an expert Maritime AI assistant used in a streaming chat UI. Follow these rules STRICTLY and stream your response token-by-token:

A. Streaming & formatting (must-follow)
1. Use Markdown for all formatting: headings (#, ##), bold (**), italics (*), lists (- or 1.), code blocks (```), and Markdown tables for tabular data.
2. NEVER generate raw HTML (no <p>, <h1>, <ul>, etc.).
3. Keep paragraphs and sentences short so streamed tokens render naturally.
4. For code snippets use fenced Markdown code blocks.
5. Structure responses logically: short executive summary, technical steps, and a Sources section at the end.
6. Final streamed message MUST include a 'Sources' section listing the exact sources used.

B. Evidence / hallucination guard (mandatory)
1. Use ONLY the Context evidence provided in the user prompt. Do NOT invent facts, part numbers, procedures, or page numbers not present in Context.
2. If Context lacks the needed info, say exactly: "I don't have enough information in the provided materials to answer that." Then ask a specific clarifying question about equipment or data needed.
3. When making factual claims, append inline provenance using the exact doc label from Context, for example: (Source: doc_id:12345) and also list the full source entries in the final 'Sources' section.

C. Source URL and citation rules (CRITICAL)
1. If a source URL appears in Context, you MUST include the EXACT FULL SOURCE URL exactly as it appears in Context when referencing it in the 'Sources' section and anywhere the user requests the source. Do NOT modify or shorten source URLs — include them exactly (including s3:// paths).
2. Every source reference MUST include a page number if page numbers are present in the Context. Use this exact citation format:  
   `Source: [EXACT_FULL_URL] (Page: [NUMBER])`  
   Example: `Source: s3://synergy-oe-propulsionpro-prod/source_data/pdf/oem/HIMSEN.PDF (Page: 123)`
3. If the Context contains only an S3 path without page numbers, still include the exact S3 path in the Sources list (without adding page numbers).

D. Charts
1. If asked to create a chart, output EXACTLY a single JSON block inside ```json fences following this schema:
{
  "data": [<numbers>],
  "type": "chart",
  "chart_type": "<line|bar|pie|doughnut|radar|scatter>",
  "title": "<chart title>",
  "labels": ["<x1>", "<x2>", "<x3>"],
  "legend": "<legend text>"
}
2. Do NOT add keys outside this schema or any prose outside the code fences.

E. Safety & tone
1. Mark safety-critical steps clearly (e.g., STOP engine before maintenance).
2. Ask the user if they want more detail, and offer next steps (e.g., show full manual page, request serial/model).
3. Keep the voice professional, concise, and evidence-driven.

If any instruction conflicts with the provided Context or would require inventing facts, follow the Evidence / hallucination guard above and refuse to invent details."""

            # Build user prompt
            # Convert equipment_context dict → string safely
            if isinstance(equipment_context, dict):
                equipment_str = "\n".join(
                    f"- {k}: {v}" for k, v in equipment_context.items() if v
                )
            else:
                equipment_str = str(equipment_context or "")

            prompt_text = (
                rephrased_query
                + "\n\n(YQL context: " + (yql or "") + ")\n\nContext:\n"
                + (context or "")
                + "\n\nEquipment Info:\n"
                + equipment_str
                + "\n\nInstructions:\n"
                + "- Provide specific guidance relevant to identified equipment when possible.\n"
                + "- Format response as structured Markdown (Short Summary / Actionable Steps / Sources).\n"
                + "- Include equipment-specific procedures and safety-critical steps when present in Context.\n"
                + "- Base your answer STRICTLY on the provided Context. DO NOT hallucinate or add facts not present.\n"
                + "- If the Context lacks the information needed, say exactly: \"I don't have enough information in the provided materials to answer that.\" Then ask a specific clarifying question (e.g., make/model/serial/page).\n"
                + "- CRITICAL: When referencing sources, you MUST include the EXACT FULL SOURCE URL exactly as provided in the Context (do NOT modify or shorten; include s3:// paths verbatim).\n"
                + "- CRITICAL: Include source references with page numbers in this exact format when page numbers are present in Context:\n"
                + "    Source: [EXACT_FULL_URL] (Page: [NUMBER])\n"
                + "  Example: Source: s3://synergy-oe-propulsionpro-prod/source_data/pdf/oem/HIMSEN.PDF (Page: 123)\n"
                + "- If equipment-specific info is unavailable in Context, provide 'General guidance (not in provided Context)' clearly labeled.\n"
                + "- Ask the user if they want more specific information about their equipment (make/model/serial/page) at the end.\n\n"
                + "MANDATORY: Every source listed in the Sources section must match exactly the URL strings shown in the Context input.\n\n"
                + "Provide a comprehensive, equipment-aware response:"
            )

            messages = [{"role": "user", "content": [{"text": prompt_text}]}]
            inference_config = {"maxTokens": 4096, "temperature": 0.7}

            # Start Bedrock streaming
            response_stream = bedrock_client.converse_stream(
                modelId=MODEL_ID,
                messages=messages,
                inferenceConfig=inference_config,
                system=[{"text": system_prompt}]
            )

            stream_iter = (
                response_stream.get("stream")
                if isinstance(response_stream, dict)
                else response_stream
            ) or []

            final_text_parts = []
            send_buffer = []
            last_send_time = time.time()

            for event in stream_iter:
                if "contentBlockDelta" not in event:
                    continue

                token = event["contentBlockDelta"]["delta"].get("text", "")
                if not token:
                    continue

                final_text_parts.append(token)
                send_buffer.append(token)

                now = time.time()
                if (
                    now - last_send_time > max_chunk_interval
                    or len("".join(send_buffer)) > max_chunk_size
                ):
                    chunk = "".join(send_buffer)
                    if not self._send_ws_chunk(connection_id, chunk,
                                               transaction_id=transaction_id,
                                               user_prompt=user_prompt,
                                               topic_id=topic_id,
                                               prompt_timestamp=prompt_timestamp,
                                               topic_name=topic_name):
                        logger.warning("Client disconnected mid-stream.")
                        # fallback: store partial response and return False so caller can fallback
                        return False
                    send_buffer = []
                    last_send_time = now

            # Final flush
            if send_buffer:
                chunk = "".join(send_buffer)
                self._send_ws_chunk(connection_id, chunk,
                                    transaction_id=transaction_id,
                                    user_prompt=user_prompt,
                                    topic_id=topic_id,
                                    prompt_timestamp=prompt_timestamp,
                                    topic_name=topic_name)

            # Final assembled text
            final_text = "".join(final_text_parts)

            # 🔥 Try to extract structured Data_json using llm_client if available
            final_data_json = []
            try:
                if hasattr(llm_client, "extract_data_json"):
                    final_data_json = llm_client.extract_data_json(final_text)
                    logger.info(f"✅ Extracted Data_json with {len(final_data_json)} items")
            except Exception as e:
                logger.warning(f"⚠️ No Data_json extracted or extraction failed: {e}")
                final_data_json = []

            # Build response_data in standard format
            response_data = self.response_formatter.create_standard_response(
                response_text=final_text,
                response_type="HTML_STREAM",
                data_json=final_data_json
            )

            # Send + persist in one place (this will add to Dynamo and send the websocket)
            self.send_websocket_response(
                connection_id=connection_id,
                transaction_id=transaction_id,
                user_prompt=user_prompt,
                topic_id=topic_id,
                prompt_timestamp=prompt_timestamp,
                response_data=response_data,
                topic_name=topic_name,
                url_mapping=url_mapping
            )
            return True


        except Exception as e:
            logger.error(f"Streaming error: {e}")
            logger.error(traceback.format_exc())
            return False

    # -------------------------------------------------------------------------
    # Response Handling
    # -------------------------------------------------------------------------
    
    def send_websocket_response(self, connection_id: str, transaction_id: str, 
                           user_prompt: str, topic_id: str, prompt_timestamp: str,
                           response_data: Dict[str, Any], topic_name: Optional[str] = None,
                           url_mapping: Optional[dict] = None) -> str:
        
        """Send final response via WebSocket and save to DynamoDB - includes optional url_mapping"""
        response_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Build websocket message
        websocket_message = {
            "transaction_id": transaction_id,
            "Prompt": user_prompt,
            "TopicID": topic_id,
            "TopicName": topic_name,
            "Prompt_timestamp": prompt_timestamp,
            "Response_timestamp": response_timestamp,
            "Response": {
                "response": response_data.get("response", ""),
                "response_type": response_data.get("response_type", "HTML_STREAM"),
                "Data_json": response_data.get("Data_json", []),
                "is_final": True
            },
            "statusCode": 200,
            "body": "Success",
        }

        # include url_mapping inside Response for structure
        if url_mapping:
            websocket_message["Response"]["url_mapping"] = url_mapping

        # Build dynamo message (strip is_final)
        dynamo_message = websocket_message.copy()
        dynamo_message["Role"] = "assistant"
        # Remove ephemeral fields not meant for DB if any (is_final)
        dynamo_message["Response"] = {k: v for k, v in websocket_message["Response"].items() if k != "is_final"}

        # Save to Dynamo in a consistent format
        dynamo_manager.add_message(topic_id, dynamo_message)

        # Send to websocket
        self.websocket_service.send_message(connection_id, json.dumps(websocket_message))
        return topic_id


    
    # -------------------------------------------------------------------------
    # NEW SESSION FLOW (FLOWCHART COMPLIANT)
    # -------------------------------------------------------------------------
    
    def handle_new_session(self, connection_id: str, transaction_id: str, 
                      user_prompt: str, topic_id: str, prompt_timestamp: str,
                      is_new_topic: bool = False) -> Dict[str, Any]:
        """Handle new session flow with FLOWCHART-COMPLIANT entity extraction"""
        print(f"🆕 NEW_SESSION: Starting entity extraction for query")
        print(f"📝 USER_QUERY: {user_prompt}")

        # ensure topic_name exists
        topic_name = None

        # -----------------------------------------------------------------
        # 🔎 Topic Name Handling (self-contained)
        # -----------------------------------------------------------------
        if is_new_topic:
            try:
                topic_name = self.topic_generator.generate_topic_name(user_prompt)
                # Persist to Dynamo safely
                try:
                    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    if hasattr(dynamo_manager, "table") and dynamo_manager.table:
                        dynamo_manager.table.update_item(
                            Key={'topicid': topic_id},
                            UpdateExpression="SET topic_name = :t, updated_at = :u",
                            ExpressionAttributeValues={':t': topic_name, ':u': timestamp}
                        )
                except Exception as pe:
                    logger.warning(f"⚠️ Failed to persist topic_name to Dynamo: {pe}")
            except Exception as gen_err:
                logger.warning(f"⚠️ Topic name generation failed: {gen_err}")
                topic_name = None
        
        # Extract entities from user query using IMPORTED entity_extractor
        entity_result = entity_extractor.process_query(user_prompt)
        print(f"🏷️ ENTITY_EXTRACTION: {entity_result}")
        
        # Check if we should proceed (using flowchart response)
        if entity_result['response'] == "True":
            print("✅ FLOWCHART: Proceeding with entity information")
            
            # Save entity info to DynamoDB
            dynamo_manager.save_entity_info(topic_id, entity_result)
            
            # Continue to main processing
            equipment_context = {
                'make': entity_result.get('make', ''),
                'model': entity_result.get('model', ''),
                'equipment': entity_result.get('equipment', ''),
                'vessel': entity_result.get('vessel', ''),
                'problems': []
            }
            
            return self.execute_main_processing_flow(
                connection_id=connection_id,
                transaction_id=transaction_id,
                user_prompt=user_prompt,
                topic_id=topic_id,
                prompt_timestamp=prompt_timestamp,
                equipment_context=equipment_context,
                rephrased_query=user_prompt,
                topic_name=topic_name
            )

        else:
            # FLOWCHART: Request more information
            print(f"❓ FLOWCHART: Requesting more entity information")
            
            response_data = self.response_formatter.create_standard_response(
                response_text=entity_result['response'],
                response_type="Text",
                data_json=[]
            )
            
            final_topic_id = self.send_websocket_response(
                connection_id, transaction_id, user_prompt, topic_id,
                prompt_timestamp, response_data
            )
            
            lambda_response = {
                "statusCode": 200, 
                "body": json.dumps({
                    "message": "OK",
                    "topic_id": final_topic_id
                })
            }
            print(f"🔚 LAMBDA_RESPONSE: {lambda_response}")
            
            return lambda_response
    
    # -------------------------------------------------------------------------
    # EXISTING SESSION FLOW
    # -------------------------------------------------------------------------
    
    def handle_existing_session(self, connection_id: str, transaction_id: str,
                              user_prompt: str, topic_id: str, prompt_timestamp: str) -> Dict[str, Any]:
        """Handle existing session flow with entity updating"""
        print(f"🔄 EXISTING_SESSION: Processing query with context")
        print(f"📝 USER_QUERY: {user_prompt}")

        # -----------------------------------------------------------------
        # 🔎 Topic Name Handling (self-contained)
        # -----------------------------------------------------------------
        
        topic_name = None
        
        try:
            if hasattr(dynamo_manager, "table") and topic_id:
                resp = dynamo_manager.table.get_item(Key={'topicid': topic_id})
                item = resp.get("Item", {}) if resp else {}
                topic_name = item.get("topic_name")
        except Exception:
            topic_name = None
        
        # Get conversation context and entities
        chat_summary = dynamo_manager.get_chat_summary(topic_id)
        equipment_context = dynamo_manager.get_recent_entities(topic_id)
        
        print(f"📋 CHAT_SUMMARY: {chat_summary}")
        
        # Ensure equipment context has proper default values
        if equipment_context is None:
            equipment_context = {}
        
        equipment_context = {
            'make': equipment_context.get('make') or '',
            'model': equipment_context.get('model') or '', 
            'equipment': equipment_context.get('equipment') or '',
            'vessel': equipment_context.get('vessel') or '',
            'problems': equipment_context.get('problems', [])
        }
        
        print(f"🔧 EXISTING_CONTEXT: {equipment_context}")
        
        # Rephrase query with context
        rephrased_query = llm_client.rephrase_query_with_context(
            user_prompt, chat_summary, equipment_context
        )
        
        print(f"🔄 REPHRASED_QUERY: {rephrased_query}")
        
        # ENTITY EXTRACTION: Extract entities from rephrased query to capture new information
        print("🏷️ ENTITY_EXTRACTION: Extracting from rephrased query...")
        entity_result = entity_extractor.process_query(rephrased_query)
        print(f"🏷️ ENTITY_EXTRACTION_RESULT: {entity_result}")
        
        # Update equipment context if new entities are found
        if entity_result['response'] == "True":
            print("🔄 UPDATING_CONTEXT: New entities found, updating equipment context...")
            
            # Merge new entities with existing context (preserve existing, add new)
            updated_context = {
                'make': entity_result.get('make') or equipment_context.get('make') or '',
                'model': entity_result.get('model') or equipment_context.get('model') or '',
                'equipment': entity_result.get('equipment') or equipment_context.get('equipment') or '',
                'vessel': entity_result.get('vessel') or equipment_context.get('vessel') or '',
                'problems': equipment_context.get('problems', [])
            }
            
            # Log what was updated
            updates = []
            for key in ['make', 'model', 'equipment', 'vessel']:
                old_val = equipment_context.get(key) or ''
                new_val = updated_context.get(key) or ''
                if new_val and new_val != old_val:
                    updates.append(f"{key}: '{old_val}' → '{new_val}'")
            
            if updates:
                print(f"✅ CONTEXT_UPDATES: {', '.join(updates)}")
                # Save updated entity info to DynamoDB and chat summary
                dynamo_manager.save_entity_info(topic_id, entity_result)
                equipment_context = updated_context
            else:
                print("ℹ️ NO_NEW_ENTITIES: No new entity information to update")
        else:
            print("ℹ️ NO_ENTITIES_EXTRACTED: No entities found in rephrased query")
        
        # Validate marine query after rephrasing (to understand context)
        if not llm_client.validate_marine_query(rephrased_query):
            print("❌ Non-marine query detected after context analysis")
            response_data = self.response_formatter.create_standard_response(
                response_text="We can't respond to your query at this moment. Please ask questions related to marine equipment.",
                response_type="Text",
                data_json=[]
            )
            final_topic_id = self.send_websocket_response(
                connection_id, transaction_id, user_prompt, topic_id,
                prompt_timestamp, response_data
            )
            
            lambda_response = {
                "statusCode": 200, 
                "body": json.dumps({
                    "message": "Non-marine query",
                    "topic_id": final_topic_id
                })
            }
            print(f"🔚 LAMBDA_RESPONSE: {lambda_response}")
            
            return lambda_response
        
        return self.execute_main_processing_flow(
            connection_id, transaction_id, user_prompt, topic_id,
            prompt_timestamp, equipment_context, rephrased_query, topic_name=topic_name
        )

    # -------------------------------------------------------------------------
    # MAIN PROCESSING FLOW (ACCEPTS PARTIAL VESPA SEARCH) + STREAMING
    # -------------------------------------------------------------------------
    
    def execute_main_processing_flow(self,
        connection_id: str,
        transaction_id: str,
        user_prompt: str,
        topic_id: str,
        prompt_timestamp: str,
        equipment_context: Dict[str, str],
        rephrased_query: str,
        topic_name: Optional[str] = None
    ) -> Dict[str, Any]:
        
        """Execute main RAG processing flow with equipment awareness and 3-tier Vespa search; streaming enabled"""
        print(f"⚙️ MAIN_PROCESSING: Starting equipment-aware RAG pipeline (streaming enabled)")
                
        # Register connection and start heartbeat to keep websocket alive during long ops
        try:
            if not hasattr(self, "connection_manager") or not self.connection_manager:
                self.connection_manager = WebSocketConnectionManager(self.websocket_service)
            self.connection_manager.register_processing_connection(connection_id, topic_id, transaction_id)
            hb_started = self.connection_manager.start_heartbeat_for_connection(connection_id)
            if not hb_started:
                logger.warning("⚠️ Failed to start heartbeat for connection - will continue but client may disconnect.")
            else:
                logger.info("✅ Heartbeat started for connection (20s interval).")
        except Exception as e:
            logger.warning(f"⚠️ Heartbeat initialization failed: {e}")

        # Ensure equipment context has proper default values
        if equipment_context is None:
            equipment_context = {}
        
        equipment_context = {
            'make': equipment_context.get('make') or '',
            'model': equipment_context.get('model') or '',
            'equipment': equipment_context.get('equipment') or '',
            'vessel': equipment_context.get('vessel') or '',
            'problems': equipment_context.get('problems') or []
        }
        
        print(f"🔧 EQUIPMENT_CONTEXT: {equipment_context}")
        
        try:
            # Step 1: LLM Query Classification (Equipment-aware)
            print("🎯 Step 1: Query Classification...")
            info_type = llm_client.classify_query_type(rephrased_query, equipment_context)
            print(f"📊 QUERY_CLASSIFICATION: {info_type}")
            
            # Step 2: Generate Embeddings
            print("🔤 Step 2: Generating embeddings...")
            query_embedding = llm_client.get_embeddings(rephrased_query)
            print(f"✅ Embeddings generated (dimension: {len(query_embedding)})")
            
            # Step 3: Execute Vespa Search with 3-tier fallback
            print("🔍 Step 3: Executing Vespa search...")
            
            # Log YQL query construction (this will be printed from vespa_client)
            search_results, yql = vespa_client.search_with_equipment_context(
                query_embedding, rephrased_query, info_type, equipment_context
            )
            
            print(f"📊 RETRIEVAL_RESULT: Found {len(search_results)} documents")
            
            # Step 4: Handle no results
            if search_results.empty:
                print("❌ No search results found after all fallback tiers")

                # Clean short-circuit response
                response_data = self.response_formatter.create_standard_response(
                    response_text="No relevant documents found. Please provide manual/page.",
                    response_type="Text",
                    data_json=[]
                )

                final_topic_id = self.send_websocket_response(
                    connection_id, transaction_id, user_prompt, topic_id,
                    prompt_timestamp, response_data
                )

                lambda_response = {
                    "statusCode": 200,
                    "body": json.dumps({
                        "message": "No results found",
                        "topic_id": final_topic_id
                    })
                }
                print(f"🔚 LAMBDA_RESPONSE: {lambda_response}")

                return lambda_response

            
            # Step 5: Prepare context from search results
            print("📋 Step 5: Preparing context from search results...")
            prepare_result = vespa_client.prepare_context(search_results)
            if isinstance(prepare_result, tuple) and len(prepare_result) == 2:
                context, url_mapping = prepare_result
            else:
                context = prepare_result
                url_mapping = {}
            print(f"📏 Context length: {len(context)} characters")
            
            # Step 6: STREAM LLM response to frontend
            print("🤖 Step 6: Streaming LLM response to frontend...")
            streaming_success = self.stream_llm_response(
                connection_id=connection_id,
                transaction_id=transaction_id,
                user_prompt=user_prompt,
                topic_id=topic_id,
                prompt_timestamp=prompt_timestamp,
                rephrased_query=rephrased_query,
                context=context,
                equipment_context=equipment_context,
                yql=yql,
                topic_name=topic_name,
                url_mapping=url_mapping
            )

            if not streaming_success:
                logger.warning("Streaming failed; falling back to single-shot generation.")
                # fallback single-shot generation (existing behavior)
                rag_response = llm_client.generate_rag_response(
                    rephrased_query + "\n" + f"YQL Query : {yql}",
                    context,
                    equipment_context
                )
                missing_entities = []
                if not equipment_context.get('make') and not equipment_context.get('model'):
                    missing_entities.extend(["make", "model"])
                elif equipment_context.get('make') and not equipment_context.get('model'):
                    missing_entities.append("model")
                elif equipment_context.get('model') and not equipment_context.get('make'):
                    missing_entities.append("make")

                final_response = llm_client.format_response_as_html(rag_response, rephrased_query, missing_entities)
                delivery_success = self.attempt_final_response_delivery(
                    connection_id, transaction_id, user_prompt, topic_id, prompt_timestamp, final_response
                )
            else:
                delivery_success = True

            if delivery_success:
                print("✅ RAG response pipeline completed successfully with delivery")
            else:
                print("💾 RAG response stored for later retrieval due to delivery issues")

            lambda_response = {
                "statusCode": 200, 
                "body": json.dumps({
                    "message": "RAG response delivered",
                    "topic_id": topic_id
                })
            }
            print(f"🔚 LAMBDA_RESPONSE: {lambda_response}")
            
            return lambda_response
            
        except Exception as e:
            logger.error(f"❌ Error in main processing flow: {str(e)}")
            logger.error(f"📍 Error traceback:")
            logger.error(traceback.format_exc())
            
            error_response = self.response_formatter.create_standard_response(
                response_text="We encountered an issue processing your request. Please try again.",
                response_type="Error",
                data_json=[]
            )
            
            final_topic_id = self.send_websocket_response(
                connection_id, transaction_id, user_prompt, topic_id,
                prompt_timestamp, error_response
            )
            
            lambda_response = {
                "statusCode": 500, 
                "body": json.dumps({
                    "message": f"Processing error: {str(e)}",
                    "topic_id": final_topic_id
                })
            }
            print(f"🔚 LAMBDA_RESPONSE: {lambda_response}")
            
            return lambda_response
    
    # -------------------------------------------------------------------------
    # Main Lambda Handler
    # -------------------------------------------------------------------------
    
    def lambda_handler(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        
        """Main AWS Lambda handler with robust input handling and server-side connection management"""
        try:
            print("🚀 Maritime RAG Lambda Handler Started with Signed URL Support")
            print("=" * 80)
            
            os.environ['INSTANCE'] = os.getenv('INSTANCE')

            print(f"Event : {event}")
            
            # ===================================================================
            # ROBUST INPUT PARSING - Handle different input formats
            # ===================================================================
            
            # Extract connection_id with fallback handling
            connection_id = None
            
            # Check if this is a full WebSocket event
            if "requestContext" in event and "connectionId" in event["requestContext"]:
                connection_id = event["requestContext"]["connectionId"]
            else:
                # Fallback for direct invocation or testing
                connection_id = event.get("connectionId", "test-connection-123")
                print(f"⚠️ No WebSocket context found, using fallback connection_id: {connection_id}")
            
            # Parse body with multiple format support
            body = None
            
            if "body" in event:
                # Handle both string and dict body formats
                if isinstance(event["body"], str):
                    try:
                        body = json.loads(event["body"])
                    except json.JSONDecodeError as e:
                        print(f"❌ JSON decode error: {str(e)}")
                        raise ValueError("Invalid JSON in body")
                else:
                    body = event["body"]
            else:
                # If no body key, assume the entire event IS the body (direct invocation)
                body = event
            
            # ===================================================================
            # EXTRACT AND VALIDATE PARAMETERS
            # ===================================================================
            
            # Extract basic parameters
            topic_id = body.get("TopicID", "")
            user_prompt = body.get("Prompt", "")
            is_new_topic_raw = body.get("IsNewTopic", False)
            
            # Robust IsNewTopic conversion - handle string "True"/"False" and boolean
            if isinstance(is_new_topic_raw, str):
                is_new_topic = is_new_topic_raw.lower() in ["true", "1", "yes"]
            else:
                is_new_topic = bool(is_new_topic_raw)
            
            # ===================================================================
            # INPUT VALIDATION
            # ===================================================================
            
            if not user_prompt or not user_prompt.strip():
                error_response = {
                    "transaction_id": str(uuid.uuid4()),
                    "Prompt": "",
                    "TopicID": topic_id,
                    "Prompt_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "Response_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "Response": {
                        "response": "Please provide a valid query about marine equipment.",
                        "response_type": "Error",
                        "Data_json": []
                    },
                    "statusCode": 400,
                    "body": "Bad Request"
                }
                
                self.websocket_service.send_message(connection_id, json.dumps(error_response))
                return {"statusCode": 400, "body": "Bad Request"}
            
            # ===================================================================
            # LOG PARSED PARAMETERS
            # ===================================================================
            
            logger.info(f"📋 LAMBDA_PARAMS: connection_id={connection_id}")
            logger.info(f"📋 SESSION_ID: {topic_id}")
            logger.info(f"📝 USER_QUERY: {user_prompt}")
            logger.info(f"🏷️ IS_NEW_TOPIC: {is_new_topic} (converted from {is_new_topic_raw})")
            
            # ===================================================================
            # GENERATE TRANSACTION METADATA
            # ===================================================================
            
            transaction_id = str(uuid.uuid4())
            prompt_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # ===================================================================
            # FLOWCHART DECISION POINT: Session Type Check
            # ===================================================================
            
            if self.is_new_session(topic_id):
                # NEW SESSION FLOW
                print("🆕 Flow: NEW SESSION")
                
                # Create new session if needed
                if not topic_id:
                    topic_id = dynamo_manager.create_chat_session()
                    print(f"🆕 Created new session: {topic_id}")
                else:
                    dynamo_manager.create_chat_session(topic_id=topic_id)
                    print(f"🆕 Created session for topic: {topic_id}")

                # Save user message
                user_message = {
                    "transaction_id": transaction_id,
                    "Prompt": user_prompt,
                    "TopicID": topic_id,
                    "Prompt_timestamp": prompt_timestamp,
                    "Role": "user"
                }
                dynamo_manager.add_message(topic_id, user_message, update_summary=False)
                
                # Handle new session flow
                result = self.handle_new_session(
                    connection_id, transaction_id, user_prompt, topic_id, 
                    prompt_timestamp, is_new_topic
                )
                
                return result
            else:
                # EXISTING SESSION FLOW
                print("🔄 Flow: EXISTING SESSION")
                
                # Save user message for existing session
                user_message = {
                    "transaction_id": transaction_id,
                    "Prompt": user_prompt,
                    "TopicID": topic_id,
                    "Prompt_timestamp": prompt_timestamp,
                    "Role": "user"
                }
                dynamo_manager.add_message(topic_id, user_message, update_summary=False)
                
                # Handle existing session flow
                result = self.handle_existing_session(
                    connection_id, transaction_id, user_prompt, topic_id, prompt_timestamp
                )
                
                return result
                
        except KeyError as e:
            logger.error(f"❌ Missing required key in event: {str(e)}")
            logger.error(f"📋 Event structure: {json.dumps(event, indent=2)}")
            
            # Send structured error response
            try:
                error_response = {
                    "transaction_id": str(uuid.uuid4()),
                    "Prompt": "",
                    "TopicID": "",
                    "Prompt_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "Response_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "Response": {
                        "response": f"Invalid event structure: missing {str(e)}",
                        "response_type": "Error", 
                        "Data_json": []
                    },
                    "statusCode": 400,
                    "body": "Bad Request"
                }
                
                if 'connection_id' in locals() and connection_id:
                    self.websocket_service.send_message(connection_id, json.dumps(error_response))
            except Exception as ws_error:
                logger.error(f"❌ Failed to send error via WebSocket: {str(ws_error)}")
            
            return {"statusCode": 400, "body": "Bad Request"}
        
        except ValueError as e:
            logger.error(f"❌ Value error in lambda_handler: {str(e)}")
            
            # Send structured error response
            try:
                error_response = {
                    "transaction_id": str(uuid.uuid4()),
                    "Prompt": "",
                    "TopicID": "",
                    "Prompt_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "Response_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "Response": {
                        "response": f"Invalid input: {str(e)}",
                        "response_type": "Error",
                        "Data_json": []
                    },
                    "statusCode": 400,
                    "body": "Bad Request"
                }
                
                if 'connection_id' in locals() and connection_id:
                    self.websocket_service.send_message(connection_id, json.dumps(error_response))
            except Exception as ws_error:
                logger.error(f"❌ Failed to send error via WebSocket: {str(ws_error)}")
            
            return {"statusCode": 400, "body": "Bad Request"}
                
        except Exception as e:
            logger.error(f"❌ Critical error in lambda_handler: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Send error response if possible
            try:
                if 'connection_id' in locals() and connection_id:
                    error_response_data = self.response_formatter.create_standard_response(
                        response_text="Sorry, we encountered a critical error. Please try again.",
                        response_type="Error",
                        data_json=[]
                    )
                    
                    error_message = {
                        "transaction_id": str(uuid.uuid4()),
                        "Prompt": user_prompt if 'user_prompt' in locals() else "",
                        "TopicID": topic_id if 'topic_id' in locals() else "",
                        "Prompt_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "Response_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "Response": error_response_data,
                        "statusCode": 500,
                        "body": "Error"
                    }
                    
                    self.websocket_service.send_message(connection_id, json.dumps(error_message))
            except Exception as ws_error:
                logger.error(f"❌ Failed to send error via WebSocket: {str(ws_error)}")
            
            return {"statusCode": 500, "body": "Error"}

# =============================================================================
# APPLICATION INSTANCE AND ENTRY POINT
# =============================================================================

# Global application instance
app = MaritimeRAGApplication()

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point"""
    return app.lambda_handler(event, context)



# =============================================================================
# MAIN EXECUTION (FOR LOCAL TESTING ONLY)
# =============================================================================

if __name__ == "__main__":
    print("🎯 Maritime RAG Application - Clean Session Management Version (Streaming integrated)")
    print("This version focuses on core functionality with clear debugging prints")
    print("Streaming enabled. All responses follow the standardized template format")
    print("=" * 80)
