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
import requests
import time
import threading
from typing import Dict, List, Optional, Tuple, Any
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import random


# Import all modules (CLEAN IMPORTS)
from config import config
from llm_client import llm_client
from dynamodb import dynamo_manager
from vespa_client import vespa_client
from entity_extraction import entity_extractor
from signed_url_generator import create_signed_url_mapping, get_signed_url_stats, replace_s3_with_signed_markdown

# AWS / model constants for streaming (adjust as needed)
AWS_REGION = "ap-south-1"
MODEL_ID = "apac.anthropic.claude-sonnet-4-20250514-v1:0"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===============================================
# USERNAME FETCH HELPER (New)
# ===============================================
def get_username_for_connection(connection_id: str) -> str:
    """Fetch username linked to this WebSocket connectionId from the $connect DynamoDB table."""
    try:
        dynamodb = boto3.resource("dynamodb")
        table_name = os.getenv("CONNECTION_TABLE_NAME")

        if not table_name:
            print("⚠️ CONNECTION_TABLE_NAME not set in environment variables.")
            return "there"

        table = dynamodb.Table(table_name)
        response = table.get_item(Key={"connectionId": connection_id})
        item = response.get("Item", {})

        username = item.get("username", "there")
        return username

    except Exception as e:
        print(f"⚠️ Failed to get username for {connection_id}: {e}")
        return "there"

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
            topic_name = llm_client.generate_claude_response(topic_prompt)

            # Clean up the topic name (remove quotes, extra whitespace, etc.)
            topic_name = topic_name.strip().strip('"').strip("'")

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
# EQUIPMENT CONTEXT NORMALIZER
# =============================================================================

def _normalize_equipment_context(raw: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Normalize equipment context dict to ensure all expected keys exist
    with safe default values. Prevents KeyError and missing-field issues.
    Also splits space-separated equipment terms into comma-separated values.
    """
    raw = raw or {}

    # Normalize engine_type hyphen to underscore
    engine_type = (raw.get("engine_type") or "").replace("-", "_")
    raw["engine_type"] = engine_type

    # 🧠 Fix: handle space-separated equipment like "starting valves exhaust valve vit system"
    eq = raw.get("equipment", "")

    return {
        'make': raw.get('make', '') or '',
        'model': raw.get('model', '') or '',
        'equipment': raw.get('equipment', '') or '',
        'vessel': raw.get('vessel', '') or '',
        'problems': raw.get('problems') or [],
        'info_type': raw.get('info_type', '') or '',
        'make_type': raw.get('make_type', '') or '',
        'detail': raw.get('detail', '') or '',
        'engine_type': raw.get('engine_type', '') or '',
    }

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

        try:
            self.bedrock_client = self.create_bedrock_client()
            logger.info("Bedrock client initialized.")
        except Exception as e:
            self.bedrock_client = None
            logger.warning("Bedrock client not initialized at startup: %s", e)

        try:
            max_workers = int(os.getenv("MAX_WORKERS", "8"))
        except Exception:
            max_workers = 8
        
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        logger.info(f"🧵 ThreadPoolExecutor initialized with max_workers={max_workers}")

        logger.info("🔥 STREAMING LOGIC: Integrated and ready (UAT app.py)")
        
        logger.info("✅ Maritime RAG Application initialized successfully")

    
    # -------------------- Background helpers --------------------
    def submit_bg(self, fn, *args, **kwargs):
        """Submit a background task and attach a logging callback; fire-and-forget style."""
        fut = self.executor.submit(fn, *args, **kwargs)

        def _log_done(f):
            try:
                res = f.result()
                logger.debug(f"✅ Background task {fn.__name__} completed successfully")
            except Exception as e:
                logger.warning(f"⚠️ Background task {fn.__name__} failed: {e}")

        fut.add_done_callback(_log_done)
        return fut

    def submit_and_wait(self, fn, *args, timeout=None, fallback=None, **kwargs):
        """
        Submit a callable and wait up to `timeout` seconds.
        If timeout occurs, returns `fallback` (default None).
        """
        fut = self.executor.submit(fn, *args, **kwargs)
        try:
            return fut.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            logger.warning(f"⏱️ Timeout waiting for {fn.__name__} (timeout={timeout}s)")
            return fallback
        except Exception as e:
            logger.error(f"❌ Error in {fn.__name__}: {e}")
            raise

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
        region = os.getenv("AWS_REGION", "ap-south-1")
        
        if aws_access_key and aws_secret_key:
            return boto3.client(
                "bedrock-runtime",
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region
            )
        
        # Default to standard boto3 client acquisition (IAM role or environment profile)
        return boto3.client("bedrock-runtime", region_name=region)
    
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

    def _dynamo_add_message_with_retry(
    self,
    topic_id,
    message_data,
    update_summary=True,
    allow_create_if_missing=False,
    max_retries=3,
    delay=0.5,
    backoff=0.25
):
        """
        Ensure add_message writes succeed (sync) with retries. Returns True on success.
        If dynamo_manager.add_message is non-blocking, prefer using submit_and_wait to wait for completion.
        """
        last_exc = None
        for attempt in range(1, max_retries + 1):
            try:
                # If add_message is synchronous, this will return only after write.
                result = dynamo_manager.add_message(topic_id, message_data, update_summary=update_summary)
                # If add_message returns a future-like object, wait for it:
                if hasattr(result, "result") and callable(result.result):
                    try:
                        result = result.result(timeout=5)
                    except Exception as e:
                        # treat as failure and retry
                        raise

                # Optionally validate result (if add_message returns success boolean)
                logger.info(f"💾 Dynamo add_message attempt {attempt} succeeded for topic {topic_id} tx={message_data.get('transaction_id')}")
                return True
            except Exception as e:
                last_exc = e
                logger.warning(f"⚠️ dynamo add_message attempt {attempt} failed for topic {topic_id}: {e}")
                time.sleep(backoff * attempt)
                continue

        logger.error(f"❌ dynamo add_message failed after {max_retries} attempts for topic {topic_id}. Last error: {last_exc}")
        return False

    def stream_llm_response(self, connection_id: str, transaction_id: str,
        user_prompt: str, topic_id: str, prompt_timestamp: str,
        rephrased_query: str, context: str, equipment_context: dict,
        yql: str, user_name: str, topic_name: Optional[str] = None, url_mapping: Optional[dict] = None, is_new_topic: bool = None,
        max_chunk_interval: float = 0.05, max_chunk_size: int = 10) -> bool:

        if url_mapping is None:
            url_mapping = {}

        """
        Stream LLM tokens to the frontend WebSocket, with buffering and simple retries.
        Falls back to single-shot if streaming fails.
        """
        try:
            bedrock_client = self.bedrock_client

            system_prompt = """You are a professional **Maritime AI Assistant** used in a streaming chat UI.
            Always answer **token-by-token** in Markdown only — never HTML.

            ## 🧩 Core Behaviour
            - Be accurate, structured, and friendly — like a **Chief Engineer mentoring a junior officer**.
            - Base answers **strictly** on the provided **Context** and **Equipment Info**.
            - If context lacks details, say exactly:
            > "I don't have enough information in the provided materials to answer that."
            Then ask a clear follow-up (e.g., make/model/page needed).

            ## 🧱 Markdown & Layout Rules
            - Use Markdown headings, bold, italics, lists, tables, and fenced code blocks.
            - No raw HTML tags.
            - Section order:
            1. ⚙️ Greeting / Intro
            2. 🧭 Overview / Summary
            3. 🔧 Maintenance / Procedures
            4. 📚 Sources
            - Separate sections with `---` and **two blank lines** before/after headings.
            - Wrap equipment names or examples in backticks (`MAN B&W`, `Turbocharger`).
            - Use **bold** for key terms and *italics* for parts or components.

            ## 🧠 Evidence & Sources
            - Use the images **in chronological order** according to their filenames. (page1_image1 → page1_image2 → page2_image1 → etc.)
            - When relevant, embed them **inline in Markdown** using:![Description](image_url)
            - Keep the image close to the related step or paragraph.
            - Do not rename or reorder the images.
            - Never invent facts or page numbers.
            - Cite sources exactly as given:
            `[s3://bucket/manual.pdf](s3://bucket/manual.pdf) (Page: 21)` - Always return only the first page number. Don't return like Page: 417-418. Return only Page: 417
            - End every reply with:
            `## 📚 Sources`
            followed by Markdown links.
            - If information is general, mark clearly:
            **General Guidance – Not in Provided Context**

            ## 📊 Charts
            If asked for charts, output only one JSON block inside ```json fences:
            {
            "data": [<numbers>],
            "type": "chart",
            "chart_type": "<line|bar|pie|doughnut|radar|scatter>",
            "title": "<chart title>",
            "labels": ["<x1>", "<x2>", "<x3>"],
            "legend": "<legend text>"
            }
            No text outside the JSON.

            ## ⚓ Tone
            - Warm, mentoring, technically precise.
            - 1–2 emojis per section (⚙️ 🚢 🧭 ✅ ❗).
            - Short sentences, max 3 lines per paragraph.
            - Example style:
            “Let’s go over this quickly ⚓”
            “Here’s what matters most 👇”
            “Alright Captain, here’s the drill 👨‍🔧”

            Respond now following these rules exactly."""

            # Build user prompt
            # Convert equipment_context dict → string safely
            if isinstance(equipment_context, dict):
                equipment_str = "\n".join(
                    f"- {k}: {v}" for k, v in equipment_context.items() if v
                )
            else:
                equipment_str = str(equipment_context or "")

            if is_new_topic:
                greeting_instruction = (
                    f"# ⚙️ Hey {user_name} 👋. I’m your Virtual Marine Assistant\n\n\n"
                    f"Welcome aboard, {user_name}! Let’s dive right in and get your MAN B&W engine running perfectly ⚓.\n\n\n"
                )
            else:
                ongoing_greetings = [
                    f"Alright {user_name} 👨‍🔧, continuing from where we left off ⚙️\n\n",
                    f"Back at it, {user_name}! Let’s fine-tune that machinery ⚙️💪\n\n",
                    f"Hey {user_name}, let’s pick up where we dropped anchor ⚓\n\n",
                    f"Good to see you again, {user_name}! Let’s keep this engine humming 🚢💨\n\n",
                    f"Alright, {user_name}! Let’s get those cylinders firing again 🔥⚙️\n\n",
                    f"Back on deck, {user_name}? Let’s steady the course and keep her running smooth 🧭⚙️\n\n"
                ]
                greeting_instruction = random.choice(ongoing_greetings)
            
            prompt_text = (
                f"{greeting_instruction}{rephrased_query}\n\n"
                "## 🧭 Context\n"
                f"{context if context else 'No context available.'}\n\n"
                "## ⚙️ Equipment Info\n"
                f"{equipment_str or 'No equipment info provided.'}\n\n"
                "## 🔧 Instructions\n"
                "- Answer concisely in Markdown only (no HTML).\n"
                "- Use structured sections: ⚙️ Intro, 🧭 Summary, 🚨 Safety, 🔧 Steps, 📚 Sources.\n"
                "- Base answers only on the above Context; never invent information.\n"
                "- If data is missing, say: "
                "\"I don't have enough information in the provided materials to answer that.\" "
                "and ask what’s missing.\n"
                "- Cite evidence as Markdown links exactly as in context "
                "(e.g., [s3://bucket/file.pdf](s3://bucket/file.pdf) (Page: 5)) - Always return only the first page number. Don't return like Page: 417-418. Return only Page: 417.\n"
                "- End your response with '## 📚 Sources'."
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

            return True, response_data

        except Exception as e:
            logger.error(f"Streaming error: {e}")
            logger.error(traceback.format_exc())
            return False, self.response_formatter.create_standard_response(
                response_text="Streaming failed. Please retry.",
                response_type="Error",
                data_json=[]
            )

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

        # Save to Dynamo in a consistent format (now asynchronous)
        try:
            try:
                success = self._dynamo_add_message_with_retry(topic_id, dynamo_message, update_summary=True)
                if not success:
                    logger.warning(f"⚠️ Failed to save assistant message for topic {topic_id}")
            except Exception as e:
                logger.error(f"❌ Exception saving assistant message for topic {topic_id}: {e}")
            try:
                # attempt to grab current known entities for the session (may be empty)
                current_entities = dynamo_manager.get_recent_entities(topic_id) or {}
                print(f"📝 _update_chat_summary_basic: appended prompt for topic {topic_id}. current_entities: {current_entities}")
            except Exception as e:
                logger.warning(f"⚠️ failed to update chat_summary with prompt: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to schedule Dynamo save for message: {e}")

        # Send to websocket
        self.websocket_service.send_message(connection_id, json.dumps(websocket_message))
        return topic_id

    def _sanitize_field(self, val: Optional[str]) -> str:
        if not val:
            return ""
        v = str(val).strip().lower()
        v = re.sub(r'[^a-z0-9\s_-]', ' ', v)   # keep alphanumerics, spaces, and hyphen
        v = re.sub(r'\s+', ' ', v).strip()    # collapse multiple spaces
        return v

    def _has_complete_equipment(self, equipment: Dict[str, str]) -> bool:
        """Return True when make, model and equipment are present (non-empty)."""
        return bool(equipment.get('make') and equipment.get('model') and equipment.get('equipment'))

    def _ensure_equipment_from_dynamo(self, topic_id: str, equipment_context: Dict[str, str]) -> Dict[str, str]:
        """Merge any already-saved recent entities from Dynamo into equipment_context."""
        try:
            recent = dynamo_manager.get_recent_entities(topic_id, consistent=True) or {}
            for k in ('make', 'model', 'equipment'):
                if not equipment_context.get(k) and recent.get(k):
                    equipment_context[k] = recent.get(k)
        except Exception as e:
            logger.debug(f"⚠️ _ensure_equipment_from_dynamo: {e}")
        return equipment_context

    def find_related_topics_by_entity(self, make: str, model: str, equipment: str,
                                    exclude_topic: Optional[str] = None, max_results: int = 10) -> List[str]:
        """
        Scan chat sessions and return topic_ids whose chat_summary.recent_info matches
        (case-insensitive) the provided make, model and equipment. Excludes exclude_topic.
        Note: this is a scan (OK for moderate table sizes). If table is huge, consider adding an index later.
        """
        try:
            make_l = (make or "").strip().lower()
            model_l = (model or "").strip().lower()
            equipment_l = (equipment or "").strip().lower()

            table = dynamo_manager.table
            kwargs = {'ProjectionExpression': 'topicid, chat_summary'}  # lightweight projection
            related = []

            resp = table.scan(**kwargs)
            items = resp.get('Items', [])
            while 'LastEvaluatedKey' in resp:
                resp = table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'], **kwargs)
                items.extend(resp.get('Items', []))

            for it in items:
                tid = it.get('topicid')
                if not tid or tid == exclude_topic:
                    continue
                recent = (it.get('chat_summary') or {}).get('recent_info') or {}
                if not recent:
                    continue

                r_make = str(recent.get('make') or "").strip().lower()
                r_model = str(recent.get('model') or "").strip().lower()
                r_equipment = str(recent.get('equipment') or "").strip().lower()

                if r_make == make_l and r_model == model_l and r_equipment == equipment_l:
                    related.append(tid)
                    if len(related) >= max_results:
                        break

            logger.info(f"🔎 find_related_topics_by_entity -> found {len(related)} topics for {make}/{model}/{equipment}")
            return related

        except Exception as e:
            logger.warning(f"⚠️ find_related_topics_by_entity failed: {e}")
            return []

    def _prompt_request_equipment_info(self, connection_id, transaction_id, user_prompt, topic_id, prompt_timestamp, username, topic_name=None):
        
        """Send the 'please provide make/model/equipment' message and return a simple Lambda response."""
        stream_chunks = [
            "# 📭 Need Equipment Details", "\n\n",
            f"Hey {username} 👋", "\n\n",
            "I need **Equipment Make**, **Model** and **Equipment Type** to search manuals and give exact procedures.\n\n",
            "- 🛠️ **Equipment Make & Model** (e.g., `MAN B&W 6S50ME-B`)\n",
            "- ⚙️ **Equipment** (e.g., `Turbocharger`, `Fuel Pump`)\n",
            "- 🚢 (Optional) Vessel Name / IMO\n\n",
            "_Send those and I’ll pull the exact pages and procedures for you._ ⚓"
        ]

        combined = "".join(stream_chunks)
        # Stream chunks so front-end sees it as an assistant streaming
        for i, chunk in enumerate(stream_chunks):
            time.sleep(0.01)
            try:
                ok = self._send_ws_chunk(
                    connection_id=connection_id,
                    text=chunk,
                    transaction_id=transaction_id,
                    user_prompt=user_prompt,
                    topic_id=topic_id,
                    prompt_timestamp=prompt_timestamp,
                    topic_name=topic_name
                )
                if not ok:
                    logger.error(f"❌ WebSocket send failed for chunk {i} — client likely disconnected.")
                    return {"statusCode": 410, "body": json.dumps({"message": "client_disconnected", "topic_id": topic_id})}
            except Exception as e:
                logger.error(f"⚠️ Streaming chunk {i} failed: {e}")
                return {"statusCode": 500, "body": json.dumps({"message": "stream_error", "topic_id": topic_id})}


        final_response = self.response_formatter.create_standard_response(
            response_text=combined,
            response_type="HTML_STREAM",
            data_json=[]
        )

        # persist to DB and send final websocket response
        self.send_websocket_response(connection_id, transaction_id, user_prompt, topic_id, prompt_timestamp, final_response, topic_name=topic_name)

        return {"statusCode": 200, "body": json.dumps({"message":"need_equipment_info","topic_id": topic_id})}

    def _truncate_text_for_model(self, text: str, max_chars: int = 6000) -> str:
        """
        Truncate long context safely for LLM model limits.
        Keep the last max_chars characters, but try to start at a sentence boundary.
        Default max_chars tuned to keep prompt under model token limits (approx).
        """
        if not text:
            return ""
        text = str(text)
        if len(text) <= max_chars:
            return text
        # Try to find a sentence boundary near the cutpoint
        start_idx = max(0, len(text) - max_chars)
        snippet = text[start_idx:]
        # Move to first newline or sentence boundary to avoid cutting words mid-sentence
        m = re.search(r'[\.\n]\s', snippet)
        if m:
            return snippet[m.end():].strip()
        # fallback: just return the last max_chars
        return snippet.strip()

    # -------------------------------------------------------------------------
    # NEW SESSION FLOW (FLOWCHART COMPLIANT)
    # -------------------------------------------------------------------------
    
    def handle_new_session(self, connection_id: str, transaction_id: str,
                        user_prompt: str, topic_id: str, prompt_timestamp: str, user_name: str,
                        is_new_topic: bool = False) -> Dict[str, Any]:
        """Handle new session flow with FLOWCHART-COMPLIANT entity extraction"""

        user_name = user_name

        # Topic Name generation
        topic_name = None
        if is_new_topic:
            try:
                topic_name = self.topic_generator.generate_topic_name(user_prompt)
                # Persist to Dynamo immediately
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
                topic_name = "Marine Equipment Query"

        # Start both tasks in parallel where possible
        times = time.time()
        entity_future = self.executor.submit(entity_extractor.process_query, user_prompt)

        # Wait for entity extraction (flow depends on result). Use a sensible timeout.
        try:
            entity_result = entity_future.result(timeout=10)
        except concurrent.futures.TimeoutError:
            logger.warning("⚠️ Entity extraction timed out; running synchronously as fallback")
            entity_result = entity_extractor.process_query(user_prompt)
        except Exception as e:
            logger.error(f"❌ Entity extraction failed: {e}")
            entity_result = {"response": "Hey there! I’m your Virtual Marine Assistant. Could you tell me the equipment make and model, vessel name, or IMO number? That’ll help me guide you more accurately.", "error": str(e)}

        print(f"⏱️ Entity Extraction took {time.time()-times:.2f}s")

        if not isinstance(entity_result, dict):
            logger.warning("⚠️ entity_result not a dict, coercing to empty dict")
            entity_result = {}
        
        if not any(entity_result.get(k) for k in ['make', 'model', 'equipment']):
            prev_context = dynamo_manager.get_recent_entities(topic_id, consistent=True)
            if prev_context:
                for key in ['make', 'model', 'equipment', 'vessel']:
                    if not entity_result.get(key):
                        entity_result[key] = prev_context.get(key)
            
        if entity_result.get('response') == "True":
            
            entity_result = _normalize_equipment_context(entity_result)
            entity_result = {k: self._sanitize_field(v) for k, v in entity_result.items()}

            dynamo_manager._update_chat_summary_basic(
                topic_id,
                current_entities=entity_result,
                problems=[],
                append_prompt=False  # avoid duplicating
            )
            logger.info("✅ Forced context sync before RAG pipeline")

            dynamo_manager.resolve_pending_contexts(topic_id, entity_result)

            try:
                make = entity_result.get('make', '') or ''
                model = entity_result.get('model', '') or ''
                equipment = entity_result.get('equipment', '') or ''
                if make and model and equipment:
                    dynamo_manager.save_entity_info(topic_id, entity_result)
                    time.sleep(0.3)
                    entity_result = dynamo_manager.get_recent_entities(topic_id, consistent=True)
                else:
                    print(f"⚠️ [INDEX] Skipping indexing — incomplete entity info") 
            except Exception as e:
                logger.debug(f"⚠️ indexing after save_entity_info failed: {e}")

            equipment_context = {
                'make': entity_result.get('make', ''),
                'model': entity_result.get('model', ''),
                'equipment': entity_result.get('equipment', ''),
                'vessel': entity_result.get('vessel', ''),
                'problems': [],
                'info_type': '',
                'make_type': entity_result.get('make_type', ''),
                'detail': entity_result.get('detail', ''),
                'engine_type': entity_result.get('engine_type', '')
            }
            
            # --- Defensive merge: ensure we have the absolute freshest persisted values
            try:
                fresh_ctx = dynamo_manager.get_recent_entities(topic_id, consistent=True) or {}
                if fresh_ctx:
                    # merge freshly persisted values into refreshed_context / equipment_context
                    for k, v in fresh_ctx.items():
                        # treat empty strings / None as not useful
                        if v and (not equipment_context.get(k) or equipment_context.get(k) != v):
                            equipment_context[k] = v
                    # normalize + sanitize after merge
                    equipment_context = _normalize_equipment_context(equipment_context)
                    equipment_context = {k: self._sanitize_field(v) for k, v in equipment_context.items()}
                    logger.info(f"🔁 Merged fresh context from Dynamo into equipment_context: {equipment_context}")
            except Exception as e:
                logger.warning(f"⚠️ Could not refresh latest entity info before continuing: {e}")

            # --- Build full chat + related sessions for richer context ---
            context_for_history = equipment_context
            chat_history_text = dynamo_manager.get_context_block_history(topic_id, context_for_history)
            if not chat_history_text:
                chat_history_text = dynamo_manager.get_recent_chat_history(topic_id, limit=2)
                if isinstance(chat_history_text, list):
                    # format list-of-messages safely
                    chat_history_text = "\n".join([m.get('Prompt') or m.get('Response') or "" for m in chat_history_text])
                if not chat_history_text:
                    chat_history = dynamo_manager.get_chat_history(topic_id) or []
                    chat_history_text = dynamo_manager._format_chat_history_for_llm(chat_history)

            # --- CONTEXT-AWARE: only use current session's context block history ---
            try:
                # Prefer context-block history scoped to the equipment_context (best-effort).
                chat_history_text = dynamo_manager.get_context_block_history(topic_id, equipment_context)
                if not chat_history_text:
                    # Fallback to full session history formatting
                    chat_history = dynamo_manager.get_chat_history(topic_id) or []
                    chat_history_text = dynamo_manager._format_chat_history_for_llm(chat_history)
                related_texts = []  # no cross-session aggregation
            except Exception as e:
                logger.warning(f"⚠️ get_context_block_history failed: {e}")
                # Fallback to previous full chat
                chat_history = dynamo_manager.get_chat_history(topic_id) or []
                chat_history_text = dynamo_manager._format_chat_history_for_llm(chat_history)
                related_texts = []

            combined_history = "\n\n".join([chat_history_text] + related_texts) if related_texts else chat_history_text
            
            # --- Rephrase query using the combined conversation history ---
            # ================================================================
            # 🧠 SMART REPHRASING CONTEXT BUILDER (last 2 user + pending + latest context)
            # ================================================================
            try:
                # 1️⃣ Get pending prompts from chat summary
                chat_summary_item = dynamo_manager.get_chat_summary(topic_id) or {}
                pending_prompts = chat_summary_item.get("pending_prompts", []) or []

                # 2️⃣ Get last 2 user messages from history
                recent_user_lines = []
                short_history = dynamo_manager.get_recent_chat_history(topic_id, limit=2) or []
                if isinstance(short_history, list):
                    for msg in short_history:
                        if isinstance(msg, dict):
                            role = msg.get("Role", "").lower()
                            text = msg.get("Prompt") or msg.get("Response") or ""
                            if role == "user" and text:
                                recent_user_lines.append(text.strip())
                        elif isinstance(msg, str):
                            recent_user_lines.append(msg.strip())
                recent_user_lines = recent_user_lines[-2:]

                # 3️⃣ Combine intelligently (pending → last2 user → current)
                combined_parts = []
                if pending_prompts:
                    combined_parts.extend(pending_prompts[-2:])
                if recent_user_lines:
                    combined_parts.extend(recent_user_lines)
                combined_parts.append(user_prompt)

                # De-duplicate and clean
                seen = set()
                combined_clean = []
                for p in combined_parts:
                    p_lower = p.lower()
                    if p_lower not in seen and p.strip():
                        combined_clean.append(p.strip())
                        seen.add(p_lower)

                combined_text = "\n".join(combined_clean)
                logger.debug(f"🧩 Rephrase context built (pending={pending_prompts[-2:]}, last2={recent_user_lines})")

            except Exception as e:
                logger.warning(f"⚠️ Failed to build combined_text for rephrasing: {e}")
                combined_text = user_prompt

            # 4️⃣ Limit combined_history to safe length (avoid Mistral 8K overflow)
            try:
                truncated_history = self._truncate_text_for_model(combined_history, max_chars=1000)
            except Exception:
                truncated_history = combined_history[:500]

            # 5️⃣ Rephrase query using the merged history + latest context
            rephrased_query = llm_client.rephrase_query_with_context(
                query=combined_text,
                chat_summary={'history_text': truncated_history},
                equipment_context=equipment_context
            )

            dynamo_manager._update_chat_summary_basic(topic_id, original_prompt=user_prompt, current_entities=equipment_context, rephrased_query=rephrased_query)

            equipment_context = _normalize_equipment_context(equipment_context)

            equipment_context = {k: self._sanitize_field(v) for k, v in equipment_context.items()}

            return self.execute_main_processing_flow(
                connection_id=connection_id,
                transaction_id=transaction_id,
                user_prompt=user_prompt,
                topic_id=topic_id,
                prompt_timestamp=prompt_timestamp,
                equipment_context=equipment_context,
                rephrased_query=rephrased_query,
                user_name = user_name,
                topic_name=topic_name,
                is_new_topic = is_new_topic
            )
        
        else:

            stream_chunks = [
                f"# ⚙️ Hey {user_name} 👋", "\n\n",
                "I'm your **Virtual Marine Assistant** ⚓\n\n",
                "Before we dive in — could you share a few quick details so I can assist precisely?\n\n",
                "- 🛠️ **Equipment Make & Model** (e.g., `MAN B&W 6S50ME-B`)\n",
                "- 🚢 **Vessel Name or IMO Number**\n",
                "- 📄 **Any specific system or component** you’re working on\n\n",
                "Once I’ve got that, I’ll pull up the exact guidance and manuals you need. ⚙️\n\n",
                "_Ready when you are, Captain._ 🧭"
            ]

            combined_string = "".join(stream_chunks)

            # 🧱 Ensure the prompt is saved even without entity info
            try:
                dynamo_manager._update_chat_summary_basic(
                    topic_id,
                    original_prompt=user_prompt,
                    current_entities={'make': None, 'model': None, 'equipment': None, 'vessel': None, 'problems': []},
                    rephrased_query=user_prompt
                )
                logger.info(f"🧱 Chat summary updated for no-entity prompt in topic {topic_id}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to save no-entity prompt: {e}")

            for i, chunk in enumerate(stream_chunks):
                time.sleep(0.01)
                try:
                    ok = self._send_ws_chunk(
                        connection_id=connection_id,
                        text=chunk,
                        transaction_id=transaction_id,
                        user_prompt=user_prompt,
                        topic_id=topic_id,
                        prompt_timestamp=prompt_timestamp,
                        topic_name=topic_name
                    )
                    if not ok:
                        logger.error(f"❌ WebSocket send failed for chunk {i} — client likely disconnected.")
                        return {"statusCode": 410, "body": json.dumps({"message": "client_disconnected", "topic_id": topic_id})}
                except Exception as e:
                    logger.error(f"⚠️ Streaming chunk {i} failed: {e}")
                    return {"statusCode": 500, "body": json.dumps({"message": "stream_error", "topic_id": topic_id})}


            final_response = self.response_formatter.create_standard_response(
                        response_text=combined_string,
                        response_type="HTML_STREAM",
                        data_json=[]
                        )

            topic_id = self.send_websocket_response(
                connection_id, transaction_id, user_prompt, topic_id,
                prompt_timestamp, final_response, topic_name=topic_name
            )

            lambda_response = {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "OK",
                    "topic_id": topic_id
                })
            }

            return lambda_response
    
    # -------------------------------------------------------------------------
    # EXISTING SESSION FLOW
    # -------------------------------------------------------------------------
    
    def handle_existing_session(self, connection_id: str, transaction_id: str,
                              user_prompt: str, topic_id: str, prompt_timestamp: str, user_name: str) -> Dict[str, Any]:
        """Handle existing session flow with entity updating"""
        print(f"📝 USER_QUERY: {user_prompt}")

        # -----------------------------------------------------------------
        # 🔎 Topic Name Handling (self-contained)
        # -----------------------------------------------------------------
        
        topic_name = None
        
        try:
            if hasattr(dynamo_manager, "table") and topic_id:
                resp = dynamo_manager.table.get_item(Key={'topicid': topic_id}, ConsistentRead=True)
                item = resp.get("Item", {}) if resp else {}
                topic_name = item.get("topic_name")
        except Exception:
            topic_name = None
        
        # Get conversation context and entities
        chat_summary = dynamo_manager.get_chat_summary(topic_id)
        equipment_context = dynamo_manager.get_recent_entities(topic_id)
        
        # Ensure equipment context has proper default values
        if equipment_context is None:
            equipment_context = {}
        
        equipment_context = {
                'make': equipment_context.get('make', ''),
                'model': equipment_context.get('model', ''),
                'equipment': equipment_context.get('equipment', ''),
                'vessel': equipment_context.get('vessel', ''),
                'problems': [],
                'info_type': equipment_context.get('info_type', ''),
                'make_type': equipment_context.get('make_type', ''),
                'detail': equipment_context.get('detail', ''),
                'engine_type': equipment_context.get('engine_type', '')
            }
        
        equipment_context = _normalize_equipment_context(equipment_context)

        equipment_context = {k: self._sanitize_field(v) for k, v in equipment_context.items()}
        
        # ENTITY EXTRACTION: Extract entities from rephrased query to capture new information
        entity_result = entity_extractor.process_query(user_prompt)
        print(f"🏷️ ENTITY_EXTRACTION_RESULT: {entity_result}")

        entity_result = _normalize_equipment_context(entity_result)
        entity_result = {k: self._sanitize_field(v) for k, v in entity_result.items()}

        # --- 🧩 Merge new entities with what’s already in Dynamo ---
        try:
            existing_ctx = dynamo_manager.get_recent_entities(topic_id) or {}
            merged_entity = existing_ctx.copy()
            for key, value in entity_result.items():
                # Only overwrite if LLM actually extracted a new value
                if value and str(value).strip():
                    merged_entity[key] = value

            # Normalize and sanitize final merged context
            merged_entity = _normalize_equipment_context(merged_entity)
            merged_entity = {k: self._sanitize_field(v) for k, v in merged_entity.items()}
            logger.info(f"🔁 Merged entity context: {merged_entity}")

            # Save merged result to Dynamo
            dynamo_manager.save_entity_info(topic_id, merged_entity)
            entity_result = merged_entity  # use merged data downstream
            try:
                dynamo_manager.resolve_pending_contexts(topic_id, {})  # clears old "spindle guide"
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"⚠️ Entity merge with Dynamo failed: {e}")

        # --- Check if previous assistant message asked for more info (e.g., "Please provide make and model")
        try:
            last_msgs = dynamo_manager.get_recent_chat_history(topic_id, limit=2) or []
            if last_msgs and isinstance(last_msgs, list):
                # Find the last assistant response
                last_assistant_msg = next(
                    (m for m in reversed(last_msgs) if m.get("Role") == "assistant"), {}
                )
                if "Please provide" in (last_assistant_msg.get("Response", {}).get("response", "") or ""):
                    # Save last user prompt as pending intent
                    last_user_msg = next(
                        (m for m in reversed(last_msgs) if m.get("Role") == "user"), {}
                    )
                    if last_user_msg:
                        pending_intent = last_user_msg.get("Prompt")
                        dynamo_manager.append_pending_prompt(topic_id, pending_intent)
        except Exception as e:
            logger.warning(f"⚠️ Failed to append pending intent: {e}")

        rephrased_query = user_prompt

        if not any(entity_result.get(k) for k in ['make', 'model', 'equipment']):
            prev_context = dynamo_manager.get_recent_entities(topic_id)
            if prev_context:
                for key in ['make', 'model', 'equipment', 'vessel']:
                    if not entity_result.get(key):
                        entity_result[key] = prev_context.get(key)
        
        # Update equipment context if new entities are found
        if any(entity_result.get(k) for k in ['make', 'model', 'equipment']):

            # Merge new entity into existing context safely
            try:
                prev_ctx = dynamo_manager.get_recent_entities(topic_id) or {}
                merged_ctx = prev_ctx.copy()
                if entity_result.get("equipment"):
                    merged_ctx["equipment"] = entity_result["equipment"]
                if entity_result.get("make"):
                    merged_ctx["make"] = entity_result["make"]
                if entity_result.get("model"):
                    merged_ctx["model"] = entity_result["model"]

                dynamo_manager._update_chat_summary_basic(
                    topic_id,
                    original_prompt=user_prompt,
                    current_entities=merged_ctx,   # merged with existing make/model
                    rephrased_query=rephrased_query
                )
            except Exception as e:
                logger.warning(f"⚠️ Failed to merge entity update in chat_summary: {e}")


            # --- Merge new entities with existing context (preserve existing, add new)
            updated_context = {
                'make': entity_result.get('make') or equipment_context.get('make', ''),
                'model': entity_result.get('model') or equipment_context.get('model', ''),
                'equipment': entity_result.get('equipment') or equipment_context.get('equipment', ''),
                'vessel': entity_result.get('vessel') or equipment_context.get('vessel', ''),
                'problems': entity_result.get('problems') or equipment_context.get('problems', []),
                'info_type': entity_result.get('info_type') or equipment_context.get('info_type', ''),
                'make_type': entity_result.get('make_type') or equipment_context.get('make_type', ''),
                'detail': entity_result.get('detail') or equipment_context.get('detail', ''),
                'engine_type': entity_result.get('engine_type') or equipment_context.get('engine_type', '')
            }

            updated_context = _normalize_equipment_context(updated_context)
            updated_context = {k: self._sanitize_field(v) for k, v in updated_context.items()}

            print(f"🔧 NORMALIZED NEW_CONTEXT (pre-save): {updated_context}")

            # --- Persist updated context ASAP so subsequent retrieval uses the new model
            # --- Persist and reverify saved context before rephrasing
            try:
                dynamo_manager.save_entity_info(topic_id, entity_result)
                time.sleep(0.3)
                entity_result = dynamo_manager.get_recent_entities(topic_id, consistent=True)
                dynamo_manager.resolve_pending_contexts(topic_id, entity_result)
                logger.info("✅ [Dynamo] entity_info saved successfully.")
            except Exception as e:
                logger.warning(f"⚠️ Failed to save entity info: {e}")

            # --- Retry-based reload to avoid stale Dynamo reads (eventual consistency)
            expected_model = (entity_result.get('model') or '').strip().lower()
            refreshed_context = None
            for attempt in range(6):  # up to ~0.6s total (6 * 0.1s)
                try:
                    # ✅ Use consistent read directly from Dynamo to avoid eventual consistency lag
                    if hasattr(dynamo_manager, "table") and dynamo_manager.table:
                        resp = dynamo_manager.table.get_item(
                            Key={'topicid': topic_id},
                            ConsistentRead=True  # 🔥 This is the key change
                        )
                        temp_item = resp.get("Item", {})
                        chat_summary = temp_item.get("chat_summary", {})
                        entity_info = temp_item.get("entity_info", {})
                        temp_ctx = {
                            'make': entity_info.get('make') or chat_summary.get('recent_info', {}).get('make'),
                            'model': entity_info.get('model') or chat_summary.get('recent_info', {}).get('model'),
                            'equipment': entity_info.get('equipment') or chat_summary.get('recent_info', {}).get('equipment'),
                            'vessel': chat_summary.get('recent_info', {}).get('vessel'),
                            'problems': chat_summary.get('recent_info', {}).get('problems', [])
                        }
                    else:
                        # fallback to normal call if table handle missing
                        temp_ctx = dynamo_manager.get_recent_entities(topic_id) or {}

                    current_model = (temp_ctx.get('model') or '').strip().lower()
                    if expected_model and current_model == expected_model:
                        refreshed_context = temp_ctx
                        logger.info(f"✅ Consistent context reloaded successfully (model={current_model}) on attempt {attempt+1}")
                        break

                except Exception as e:
                    logger.debug(f"Retry {attempt+1}: consistent get_recent_entities failed ({e})")

                time.sleep(0.1)

            if not refreshed_context:
                logger.warning(f"⚠️ Using local updated_context due to potential Dynamo lag")
                refreshed_context = updated_context

            refreshed_context = _normalize_equipment_context(refreshed_context)
            refreshed_context = {k: self._sanitize_field(v) for k, v in refreshed_context.items()}
            print(f"♻️ FINAL REFRESHED CONTEXT (stable): {refreshed_context}")

            # --- Force reload of recently-saved entities to ensure retrieval uses the latest values
            try:
                refreshed_context = dynamo_manager.get_recent_entities(topic_id) or updated_context
            except Exception as e:
                logger.warning(f"⚠️ get_recent_entities failed, falling back to updated_context: {e}")
                refreshed_context = updated_context

            refreshed_context = _normalize_equipment_context(refreshed_context)
            refreshed_context = {k: self._sanitize_field(v) for k, v in refreshed_context.items()}
            print(f"♻️ RELOADED CONTEXT POST-DYNAMO: {refreshed_context}")

            # --- Build chat history for rephrasing, prefer context-block history scoped to refreshed_context
            try:
                chat_history_text = dynamo_manager.get_context_block_history(topic_id, refreshed_context)
                if not chat_history_text:
                    chat_history = dynamo_manager.get_chat_history(topic_id) or []
                    chat_history_text = dynamo_manager._format_chat_history_for_llm(chat_history)
            except Exception as e:
                logger.warning(f"⚠️ get_context_block_history failed: {e}")
                chat_history = dynamo_manager.get_chat_history(topic_id) or []
                chat_history_text = dynamo_manager._format_chat_history_for_llm(chat_history)

            # --- Include unresolved pending prompts (carry-forward intent)
            try:
                chat_summary_item = dynamo_manager.get_chat_summary(topic_id) or {}
                pending_prompts = chat_summary_item.get("pending_prompts", []) or []
            except Exception as e:
                logger.warning(f"⚠️ get_chat_summary failed: {e}")
                pending_prompts = []

            # --- Build combined_text for rephrase:
            try:
                recent_user_lines = []
                short_history = dynamo_manager.get_recent_chat_history(topic_id, limit=2) or []

                # get_recent_chat_history should return a list of message dicts; if it returns text, adapt accordingly
                if isinstance(short_history, list):
                    for msg in short_history:
                        if isinstance(msg, dict):
                            role = msg.get("Role", "").lower()
                            text = msg.get("Prompt") or msg.get("Response", "") or ""
                            if role == "user" and text:
                                recent_user_lines.append(text)
                        elif isinstance(msg, str):
                            recent_user_lines.append(msg)
                recent_user_lines = recent_user_lines[-2:]

                pending_last = (pending_prompts or [])[-2:]  # take at most last 2 pending prompts

                combined_parts = []

                if pending_last:
                    combined_parts.extend(pending_last)

                if recent_user_lines:
                    combined_parts.extend([line for line in recent_user_lines if line not in pending_last])

                if user_prompt not in combined_parts:
                    combined_parts.append(user_prompt)

                combined_text = "\n".join(combined_parts)
                logger.debug(f"🧠 Carrying forward pending prompts into rephrasing (last2): {pending_last} / recent_user_lines: {recent_user_lines}")
            except Exception as e:
                logger.warning(f"⚠️ Error building combined_text for rephrase: {e}")
                combined_text = user_prompt

            related_texts = []  # no cross-session aggregation
            combined_history = "\n\n".join([chat_history_text] + related_texts) if related_texts else chat_history_text

            combined_text = combined_text.strip()
            if len(combined_text.split()) < 3:
                combined_text = f"{user_prompt}"  # fallback to raw if too short

            try:
                truncated_history = self._truncate_text_for_model(combined_history, max_chars=1000)
            except Exception:
                truncated_history = combined_history[:500]

            # --- Rephrase using combined_text (preserves pending intent) and refreshed_context (new model)
            rephrased_query = llm_client.rephrase_query_with_context(
                query=user_prompt.strip(),
                chat_summary={'history_text': truncated_history},
                equipment_context=refreshed_context
            )

            print(f"🔄 REPHRASED_QUERY: {rephrased_query}")

            try:
                fresh_ctx = dynamo_manager.get_recent_entities(topic_id, consistent=True) or {}
                if fresh_ctx:
                    # merge freshly persisted values into refreshed_context / equipment_context
                    for k, v in fresh_ctx.items():
                        if v and not refreshed_context.get(k):
                            refreshed_context[k] = v
            except Exception as e:
                logger.warning(f"⚠️ Could not refresh latest entity info before updating chat summary: {e}")

            # --- Detect and persist any context updates (old -> new)
            updates = []
            for key in ['make', 'model', 'equipment', 'vessel']:
                old_val = equipment_context.get(key) or ''
                new_val = refreshed_context.get(key) or ''
                if new_val and new_val != old_val:
                    updates.append(f"{key}: '{old_val}' → '{new_val}'")

        else:
            try:

                dynamo_manager._update_chat_summary_basic(
                    topic_id,
                    original_prompt=user_prompt,
                    current_entities=refreshed_context,
                    rephrased_query=rephrased_query
                )
                logger.info(f"🧱 Chat summary updated for non-entity prompt (existing session), topic {topic_id}")
            except Exception as e:
                logger.warning(f"⚠️ Summary update failed for non-entity prompt: {e}")        
        
        # Validate marine query after rephrasing (to understand context)
        if not llm_client.validate_marine_query(rephrased_query):

            # Create a styled Markdown streaming message
            stream_chunks = [
                "# 🚫 Non-Marine Query Detected", "\n\n",
                f"Hey {user_name} 👋", "\n\n",
                "I'm your **Virtual Marine Assistant** ⚓ — looks like this question isn’t about ship systems or marine equipment.", "\n\n",
                "Please try asking something related to:", "\n\n",
                "- ⚙️ **Main engine operations** (e.g., MAN B&W 6S50ME-B)\n",
                "- ⚓ **Auxiliary systems** (cooling, lubrication, fuel, etc.)\n",
                "- 🧭 **Maintenance, troubleshooting, or procedures**\n",
                "- 🚢 **Port operations or vessel performance**\n\n",
                "_Once you’re back on course, I’ll have the right technical guidance ready._ 🧠\n\n",
                "Let’s steer the conversation back to marine topics! ⚙️"
            ]

            combined_string = "".join(stream_chunks)

            # Stream the formatted chunks (with the same cadence)
            for i, chunk in enumerate(stream_chunks):
                time.sleep(0.01)
                try:
                    ok = self._send_ws_chunk(
                        connection_id=connection_id,
                        text=chunk,
                        transaction_id=transaction_id,
                        user_prompt=user_prompt,
                        topic_id=topic_id,
                        prompt_timestamp=prompt_timestamp,
                        topic_name=topic_name
                    )
                    if not ok:
                        logger.error(f"❌ WebSocket send failed for chunk {i} — client likely disconnected.")
                        return {"statusCode": 410, "body": json.dumps({"message": "client_disconnected", "topic_id": topic_id})}
                except Exception as e:
                    logger.error(f"⚠️ Streaming chunk {i} failed: {e}")
                    return {"statusCode": 500, "body": json.dumps({"message": "stream_error", "topic_id": topic_id})}


            final_response = self.response_formatter.create_standard_response(
                        response_text=combined_string,
                        response_type="HTML_STREAM",
                        data_json=[]
                        )

            topic_id = self.send_websocket_response(
                connection_id, transaction_id, user_prompt, topic_id,
                prompt_timestamp, final_response, topic_name=topic_name
            )

            # Build the final response object
            lambda_response = {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Non-marine query",
                    "topic_id": topic_id
                })
            }

            return lambda_response

        try:

            dynamo_manager._update_chat_summary_basic(
                topic_id,
                original_prompt=user_prompt,
                equipment_context=refreshed_context,
                rephrased_query=rephrased_query
            )
            logger.info(f"🧱 Final chat_summary update done for topic {topic_id}")
        except Exception as e:
            logger.warning(f"⚠️ Final summary update failed: {e}")
        
        return self.execute_main_processing_flow(
            connection_id, transaction_id, user_prompt, topic_id,
            prompt_timestamp, refreshed_context, rephrased_query, 
            user_name=user_name, topic_name=topic_name
        )
    
    def execute_main_processing_flow(self,
            connection_id: str,
            transaction_id: str,
            user_prompt: str,
            topic_id: str,
            prompt_timestamp: str,
            equipment_context: Dict[str, str],
            rephrased_query: str,
            user_name: str,
            topic_name: str = None,
            is_new_topic: bool = False
        ) -> Dict[str, Any]:

            """Execute main RAG processing flow with equipment awareness and 3-tier Vespa search; streaming enabled"""

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
                'problems': equipment_context.get('problems') or [],
                'info_type': equipment_context.get('info_type') or '',
                'make_type': equipment_context.get('make_type') or '',
                'detail': equipment_context.get('detail') or '',
                'engine_type': equipment_context.get('engine_type') or ''
            }

            equipment_context = _normalize_equipment_context(equipment_context)

            print(f"🔧 EQUIPMENT_CONTEXT: {equipment_context}")

            equipment_context = self._ensure_equipment_from_dynamo(topic_id, equipment_context)

            # --- Defensive merge: ensure we have the absolute freshest persisted values
            try:
                fresh_ctx = dynamo_manager.get_recent_entities(topic_id, consistent=True) or {}
                if fresh_ctx:
                    # merge freshly persisted values into refreshed_context / equipment_context
                    for k, v in fresh_ctx.items():
                        # treat empty strings / None as not useful
                        if v and (not equipment_context.get(k) or equipment_context.get(k) != v):
                            equipment_context[k] = v
                    # normalize + sanitize after merge
                    equipment_context = _normalize_equipment_context(equipment_context)
                    equipment_context = {k: self._sanitize_field(v) for k, v in equipment_context.items()}
                    logger.info(f"🔁 Merged fresh context from Dynamo into equipment_context: {equipment_context}")
            except Exception as e:
                logger.warning(f"⚠️ Could not refresh latest entity info before continuing: {e}")

            print(f"🔧 EQUIPMENT_CONTEXT (post-dynamo-merge): {equipment_context}")

            # If we still don't have make/model/equipment, ask for them and stop early.
            if not self._has_complete_equipment(equipment_context):
                logger.info("🔎 Missing make/model/equipment - preserving partial context and requesting details")
                # Save partial context so it’s available on next query
                try:
                    dynamo_manager._update_chat_summary_basic(
                        topic_id,
                        original_prompt=user_prompt,
                        equipment_context=_normalize_equipment_context(equipment_context),
                        rephrased_query=rephrased_query
                    )
                    logger.info(f"🧭 Saved partial context before equipment info request for topic {topic_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to save partial context before equipment info request: {e}")

                return self._prompt_request_equipment_info(
                    connection_id, transaction_id, user_prompt, topic_id, prompt_timestamp, user_name, topic_name
                )

            try:
                t0 = time.time()
                # Step 1 & 2: Query classification and embeddings - run concurrently
                classify_future = self.executor.submit(llm_client.classify_query_type, rephrased_query, equipment_context)
                embed_future = self.executor.submit(llm_client.get_embeddings, rephrased_query)
                print(f"⏱️ Embeddings took {time.time()-t0:.2f}s")

                context_history_text = dynamo_manager.get_context_block_history(topic_id, equipment_context)
                
                # Wait for embeddings (required), allow a generous timeout
                try:
                    query_embedding = embed_future.result(timeout=30)
                except concurrent.futures.TimeoutError:
                    logger.warning("⚠️ Embeddings generation timed out - retrying synchronously as fallback")
                    query_embedding = llm_client.get_embeddings(rephrased_query)
                except Exception as e:
                    logger.error(f"❌ Embeddings generation failed: {e}")
                    raise

                # Try to fetch classification quickly; fallback to None if it takes too long
                try:
                    info_type = classify_future.result(timeout=2)
                except concurrent.futures.TimeoutError:
                    logger.info("ℹ️ Classification not ready quickly; proceeding without it")
                    info_type = None
                except Exception:
                    info_type = None

                print(f"📊 QUERY_CLASSIFICATION: {info_type}")

                # Step 3: Execute Vespa search (offload to thread pool to avoid blocking main thread)
                tsearch_start = time.time()
                search_results, yql = vespa_client.search_tiers_parallel(
                    query_embedding, user_prompt, info_type, equipment_context, timeout=12.0
                )
                print(f"⏱️ Vespa parallel search took {time.time()-tsearch_start:.2f}s")
                print(f"Vespa YQL: {yql}")
                print(f"📊 RETRIEVAL_RESULT: Found {len(search_results) if hasattr(search_results,'__len__') else 'N/A'} documents")

                # Step 4: Handle no results
                if getattr(search_results, "empty", False):

                    # Styled Markdown streaming message for no results
                    stream_chunks = [
                        "# 📭 No Relevant Documents Found", "\n\n",
                        f"Hey {user_name} 👋", "\n\n",
                        "I searched across all document tiers and couldn’t find anything that matches your query. 🧭", "\n\n",
                        "To help me locate the exact manual or section, please share any of the following:", "\n\n",
                        "- 📘 **Manual Name or File ID** (e.g., `S50ME-B_01`)\n",
                        "- 📄 **Page Number or Section Title** (if known)\n",
                        "- ⚙️ **Specific Equipment or Component** (e.g., `Turbocharger`, `Fuel Pump`)\n\n",
                        "Once you provide that, I’ll pull up the precise reference and operation details for you. ⚓", "\n\n",
                        "_Let’s zero in on the right document together, Chief._ 🧠"
                    ]

                    combined_string = "".join(stream_chunks)

                    # Stream chunks just like the marine assistant greeting
                    for i, chunk in enumerate(stream_chunks):
                        time.sleep(0.01)
                        try:
                            ok = self._send_ws_chunk(
                                connection_id=connection_id,
                                text=chunk,
                                transaction_id=transaction_id,
                                user_prompt=user_prompt,
                                topic_id=topic_id,
                                prompt_timestamp=prompt_timestamp,
                                topic_name=topic_name
                            )
                            if not ok:
                                logger.error(f"❌ WebSocket send failed for chunk {i} — client likely disconnected.")
                                return {"statusCode": 410, "body": json.dumps({"message": "client_disconnected", "topic_id": topic_id})}
                        except Exception as e:
                            logger.error(f"⚠️ Streaming chunk {i} failed: {e}")
                            return {"statusCode": 500, "body": json.dumps({"message": "stream_error", "topic_id": topic_id})}


                    final_response = self.response_formatter.create_standard_response(
                        response_text=combined_string,
                        response_type="HTML_STREAM",
                        data_json=[]
                        )

                    final_topic_id = self.send_websocket_response(
                        connection_id, transaction_id, user_prompt, topic_id,
                        prompt_timestamp, final_response, topic_name=topic_name
                    )

                    dynamo_manager._update_chat_summary_basic(topic_id, original_prompt=rephrased_query, current_entities=equipment_context, rephrased_query=rephrased_query)

                    lambda_response = {
                        "statusCode": 500,
                        "body": json.dumps({
                            "message": "Processing error: no search results",
                            "topic_id": final_topic_id
                        })
                    }

                    return lambda_response

                # Step 5: Prepare context (async)
                prepare_future = self.executor.submit(vespa_client.prepare_context, search_results)
                try:
                    t2 = time.time()
                    prepare_result = prepare_future.result(timeout=20)
                    print(f"⏱️ Context prep took {time.time()-t2:.2f}s")
                except concurrent.futures.TimeoutError:
                    logger.warning("⚠️ Context preparation timed out - running synchronously as fallback")
                    prepare_result = vespa_client.prepare_context(search_results)
                except Exception as e:
                    logger.error(f"❌ Context preparation failed: {e}")
                    raise

                if isinstance(prepare_result, tuple) and len(prepare_result) == 2:
                    context, url_mapping = prepare_result
                else:
                    context = prepare_result
                    url_mapping = {}

                print("🔍 CONTEXT SAMPLE:\n", prepare_result[0][:1500])
                print("🔍 URL MAPPING KEYS:", list(prepare_result[1].keys())[:10])

                # Step 6: Start signed URL generation in background (if there is a mapping)
                signed_future = None
                try:
                    if url_mapping:
                        # Handle dict or list
                        s3_urls = list(url_mapping.values()) if isinstance(url_mapping, dict) else url_mapping
                        signed_future = self.submit_bg(create_signed_url_mapping, url_mapping)
                except Exception as e:
                    logger.warning(f"⚠️ Signed URL generation scheduling failed: {e}")

                # Step 7: STREAM LLM response to frontend (do not wait for signed URLs)
                t3 = time.time()
                print(context[:4000])
                success, response_data = self.stream_llm_response(
                    connection_id=connection_id,
                    transaction_id=transaction_id,
                    user_prompt=user_prompt,
                    topic_id=topic_id,
                    prompt_timestamp=prompt_timestamp,
                    rephrased_query=rephrased_query,
                    context=context,
                    equipment_context=equipment_context,
                    yql=yql,
                    user_name = user_name,
                    topic_name=topic_name,
                    url_mapping=url_mapping,
                    is_new_topic=is_new_topic  # start streaming now; attach real signed urls later
                )
                print(f"⏱️ Streaming took {time.time()-t3:.2f}s")

                if signed_future:
                    s3_urls = list(url_mapping.keys()) if isinstance(url_mapping, dict) else url_mapping
                    signed_map = signed_future.result(timeout=30)
                    if signed_map:
                        # 🔥 Step 4: Replace inside final text
                        try:
                            final_response_text = response_data.get("response", "")
                            final_response_text = replace_s3_with_signed_markdown(final_response_text, signed_map)
                            response_data["response"] = final_response_text
                        except Exception as e:
                            logger.warning(f"⚠️ Failed to replace s3 links in text: {e}")
                        
                        # Attach signed map to client
                        # 🔥 Send updated final response with signed URLs
                        updated_resp = self.response_formatter.create_standard_response(
                            response_text=response_data["response"],   # now contains replaced links
                            response_type="HTML_STREAM",
                            data_json=response_data.get("Data_json", [])
                        )

                        self.send_websocket_response(
                            connection_id=connection_id,
                            transaction_id=transaction_id,
                            user_prompt=user_prompt,
                            topic_id=topic_id,
                            prompt_timestamp=prompt_timestamp,
                            response_data=updated_resp,
                            topic_name=topic_name,
                            url_mapping=signed_map
                        )

                if not success:
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

                return lambda_response

            except Exception as e:
                logger.error(f"❌ Error in main processing flow: {str(e)}")
                logger.error(f"📍 Error traceback:")
                logger.error(traceback.format_exc())

                stream_chunks = [
                    "# ⚠️ System Error Encountered", "\n\n",
                    f"Hey {user_name} 👋", "\n\n",
                    "I ran into a small hiccup while processing your request. 🧭", "\n\n",
                    "Don’t worry — your data’s safe, but something went off course. ⚓", "\n\n",
                    "Here’s what you can do next:", "\n\n",
                    "- 🔁 Try resubmitting your query in a few moments.\n",
                    "- 🧠 If it keeps happening, share a bit more context — like the equipment name or the document reference (e.g., `MAN B&W`).\n",
                    "- 📞 Optionally, report this issue with your session ID for diagnostics.\n\n",
                    "_I’ll get us back on course as soon as possible, Chief._ 🛠️"
                ]

                combined_string = "".join(stream_chunks)

                for i, chunk in enumerate(stream_chunks):
                    time.sleep(0.01)
                    try:
                        ok = self._send_ws_chunk(
                            connection_id=connection_id,
                            text=chunk,
                            transaction_id=transaction_id,
                            user_prompt=user_prompt,
                            topic_id=topic_id,
                            prompt_timestamp=prompt_timestamp,
                            topic_name=topic_name
                        )
                        if not ok:
                            logger.error(f"❌ WebSocket send failed for chunk {i} — client likely disconnected.")
                            return {"statusCode": 410, "body": json.dumps({"message": "client_disconnected", "topic_id": topic_id})}
                    except Exception as e:
                        logger.error(f"⚠️ Streaming chunk {i} failed: {e}")
                        return {"statusCode": 500, "body": json.dumps({"message": "stream_error", "topic_id": topic_id})}

                final_response = self.response_formatter.create_standard_response(
                        response_text=combined_string,
                        response_type="HTML_STREAM",
                        data_json=[]
                        )

                topic_id = self.send_websocket_response(
                    connection_id, transaction_id, user_prompt, topic_id,
                    prompt_timestamp, final_response, topic_name=topic_name
                )

                lambda_response = {
                    "statusCode": 200,
                    "body": json.dumps({
                        "message": "No results found",
                        "topic_id": topic_id
                    })
                }

                return lambda_response

    
    # -------------------------------------------------------------------------
    # Main Lambda Handler
    # -------------------------------------------------------------------------
    
    def lambda_handler(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        
        """Main AWS Lambda handler with robust input handling and server-side connection management"""
        try:
            
            os.environ['INSTANCE'] = os.getenv('INSTANCE')
            
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
            # STEP 3: Fetch user details from DynamoDB instead of token API
            # ===================================================================
            try:
                username = get_username_for_connection(connection_id)
                if not username or username.lower() == "there":
                    username = "there"
            except Exception as e:
                username = "there"

            
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
                print("🆕 Flow: NEW SESSION")

                # Create a new session only if topic_id is missing or invalid
                if not topic_id:
                    topic_id = dynamo_manager.create_chat_session()
                else:
                    # Ensure create_chat_session returns the actual topic id used (may return same or new)
                    try:
                        returned_tid = dynamo_manager.create_chat_session(topic_id=topic_id)
                        if returned_tid:
                            topic_id = returned_tid
                    except Exception as e:
                        logger.warning(f"⚠️ create_chat_session(topic_id=...) failed to return id: {e}")

                # Save user message
                user_message = {
                    "transaction_id": transaction_id,
                    "Prompt": user_prompt,
                    "TopicID": topic_id,
                    "Prompt_timestamp": prompt_timestamp,
                    "Role": "user"
                }
                
                # non-blocking write
                # Synchronous/confirmed write with retries — ensure summary update for user messages
                write_ok = self._dynamo_add_message_with_retry(topic_id, user_message, update_summary=True, allow_create_if_missing=True)
                if not write_ok:
                    logger.warning(f"⚠️ Failed to persist user message for topic {topic_id} (tx={transaction_id}) — continuing but message may be lost")

                try:
                    # attempt to grab current known entities for the session (may be empty)
                    current_entities = dynamo_manager.get_recent_entities(topic_id) or {}
                    print(f"📝 _update_chat_summary_basic: appended prompt for topic {topic_id}. current_entities: {current_entities}")
                except Exception as e:
                    logger.warning(f"⚠️ failed to update chat_summary with prompt: {e}")

                # 🔥 Send immediate ACK before the heavy processing starts
                ack = {
                    "transaction_id": transaction_id,
                    "Prompt": user_prompt,
                    "TopicID": topic_id,
                    "Prompt_timestamp": prompt_timestamp,
                    "Response_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "Response": {
                        "response": "",
                        "response_type": "HTML_STREAM",
                        "Data_json": []
                    },
                    "statusCode": 102,
                    "body": "Processing"
                }
                
                ack["TopicID"] = topic_id 
                
                try:
                    self.websocket_service.send_message(connection_id, json.dumps(ack))
                    logger.info(f"📤 Immediate ACK sent to {connection_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to send immediate ACK: {e}")

                # Handle new session flow
                result = self.handle_new_session(
                    connection_id, transaction_id, user_prompt, topic_id, 
                    prompt_timestamp, user_name = username, is_new_topic = True
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
                
                # non-blocking write
                # Synchronous/confirmed write with retries — ensure summary update for user messages
                write_ok = self._dynamo_add_message_with_retry(topic_id, user_message, update_summary=True, allow_create_if_missing=False)
                if not write_ok:
                    logger.warning(f"⚠️ Failed to persist user message for topic {topic_id} (tx={transaction_id}) — continuing but message may be lost")

                try:
                    # attempt to grab current known entities for the session (may be empty)
                    current_entities = dynamo_manager.get_recent_entities(topic_id) or {}
                    print(f"📝 _update_chat_summary_basic: appended prompt for topic {topic_id}. current_entities: {current_entities}")
                except Exception as e:
                    logger.warning(f"⚠️ failed to update chat_summary with prompt: {e}")

                # 🔥 Send immediate ACK before the heavy processing starts
                ack = {
                    "transaction_id": transaction_id,
                    "Prompt": user_prompt,
                    "TopicID": topic_id,
                    "Prompt_timestamp": prompt_timestamp,
                    "Response_timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "Response": {
                        "response": "",
                        "response_type": "HTML_STREAM",
                        "Data_json": []
                    },
                    "statusCode": 102,
                    "body": "Processing"
                }
                
                try:
                    self.websocket_service.send_message(connection_id, json.dumps(ack))
                    logger.info(f"📤 Immediate ACK sent to {connection_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to send immediate ACK: {e}")
                
                # Handle existing session flow
                result = self.handle_existing_session(
                    connection_id, transaction_id, user_prompt, topic_id, prompt_timestamp, user_name = username
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