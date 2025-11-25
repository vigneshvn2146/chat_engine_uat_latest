"""
DynamoDB Chat History Module

This module provides a complete implementation for storing and retrieving
chat history in DynamoDB, including LLM-based summarization using Mistral.
"""

import os
import time
import boto3
import json
import uuid
import datetime
import logging
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from config import config
from typing import Dict, List, Optional, Tuple, Any

# Configure logging
logger = logging.getLogger(__name__)

deserializer = TypeDeserializer()

class DynamoDBChatManager:
    """
    Class to handle DynamoDB operations for chat history management with Mistral LLM integration
    """
    def __init__(self):
        """Initialize DynamoDB client and table references"""
        self.aws_credentials = config.get_aws_credentials()
        self.table_name = config.table_name

        # Initialize DynamoDB client
        self.dynamodb = boto3.resource(
            'dynamodb',
            **self.aws_credentials
        )
        
        # Get table reference
        self.table = self.dynamodb.Table(self.table_name)
        print(f"✅ Connected to DynamoDB table: {self.table.name}")
        
        # Ensure table exists or create it
        self.ensure_table_exists()
    
    def ensure_table_exists(self):
        """Check if table exists, create if it doesn't"""
        try:
            # Check if table exists
            self.dynamodb.meta.client.describe_table(TableName=self.table_name)
            logger.info(f"✅ Table {self.table_name} already exists")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info(f"📋 Table {self.table_name} not found, creating...")
                # Create the table
                table = self.dynamodb.create_table(
                    TableName=self.table_name,
                    KeySchema=[
                        {'AttributeName': 'topicid', 'KeyType': 'HASH'}  # Primary key with correct name
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'topicid', 'AttributeType': 'S'}
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )
                # Wait for table to be created
                table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
                logger.info(f"✅ Table {self.table_name} created successfully")
            else:
                logger.error(f"❌ Error checking/creating table: {str(e)}")
                raise
    
    def create_chat_session(self, topic_id=None, topic_name=None, user_id=None):
        """
        Create or reuse a chat session safely.
        - If topic_id provided and exists → reuse it (idempotent).
        - If topic_id provided but not found → create new item using the same id.
        - If topic_id not provided → generate a new one.
        Always returns the effective topic_id used.
        """
        try:
            # Step 1️⃣ Check if topic_id exists
            if topic_id:
                try:
                    resp = self.table.get_item(Key={'topicid': topic_id})
                    if "Item" in resp and resp["Item"]:
                        logger.info(f"ℹ️ Existing chat session found for topic_id: {topic_id}")
                        return topic_id  # idempotent reuse
                except Exception as e:
                    logger.warning(f"⚠️ get_item check failed for {topic_id}: {e}")

            # Step 2️⃣ Generate a new topic_id if not provided or not found
            if not topic_id:
                topic_id = str(uuid.uuid4())

            # Step 3️⃣ Build initial session item
            timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            item = {
                'topicid': topic_id,
                'chat-history': [],
                'chat_summary': {
                    'navigation_flow': [],
                    'recent_info': {
                        'make': None,
                        'model': None,
                        'vessel': None,
                        'equipment': None,
                        'problems': []
                    },
                    'context_blocks': [],
                    'pending_prompts': [],
                    'last_updated': timestamp
                },
                'entity_info': {
                    'make': '',
                    'model': '',
                    'equipment': '',
                    'last_updated': timestamp
                },
                'created_at': timestamp,
                'updated_at': timestamp
            }

            if user_id:
                item['user_id'] = user_id
            if topic_name:
                item['topic_name'] = topic_name

            # Step 4️⃣ Use conditional put to avoid overwriting existing record
            self.table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(topicid)'  # ensures no overwrite
            )
            logger.info(f"✅ Created new chat session (topic_id={topic_id})")
            return topic_id

        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            # This means the item already existed — just reuse it
            logger.info(f"♻️ Chat session already exists, reusing topic_id={topic_id}")
            return topic_id

        except Exception as e:
            logger.error(f"❌ Error creating chat session: {str(e)}")
            raise
    
    def add_message(self, topic_id, message_data, update_summary=True, allow_create_if_missing=False):
        """
        Add a new message to an existing chat session.
        - If session not found and allow_create_if_missing=True → create it.
        - If session not found and allow_create_if_missing=False → raise error (no silent creation).
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            # Try to fetch existing chat history
            response = self.table.get_item(Key={'topicid': topic_id})

            if 'Item' not in response:
                if allow_create_if_missing:
                    logger.warning(f"⚠️ Chat session {topic_id} not found, creating new session.")
                    self.create_chat_session(topic_id=topic_id)
                    chat_history = []
                else:
                    logger.error(f"❌ Chat session {topic_id} missing — refusing to auto-create.")
                    raise ValueError(f"Chat session {topic_id} not found or expired.")
            else:
                chat_history = response['Item'].get('chat-history', [])

            # Append message
            chat_history.append(message_data)

            # Persist new message list
            self.table.update_item(
                Key={'topicid': topic_id},
                UpdateExpression="SET #ch = :chat_history, #ua = :updated_at",
                ExpressionAttributeNames={
                    '#ch': 'chat-history',
                    '#ua': 'updated_at'
                },
                ExpressionAttributeValues={
                    ':chat_history': chat_history,
                    ':updated_at': timestamp
                }
            )

            # Optionally trigger summary update
            if update_summary and message_data.get("Role") == "assistant":
                self.update_summary_with_mistral(topic_id)

            logger.info(f"✅ Added message to chat session {topic_id}")
            return True

        except ValueError as ve:
            logger.error(f"🚫 {ve}")
            raise

        except Exception as e:
            logger.error(f"❌ Error adding message to chat session {topic_id}: {str(e)}")
            raise
    
    def get_chat_history(self, topic_id):
        """Retrieve chat history for a specific topic"""
        try:
            response = self.table.get_item(Key={'topicid': topic_id})
            
            if 'Item' not in response:
                logger.warning(f"⚠️ Chat session {topic_id} not found")
                return []
            
            return response['Item'].get('chat-history', [])
        except Exception as e:
            logger.error(f"❌ Error retrieving chat history: {str(e)}")
            raise
    
    def get_chat_summary(self, topic_id):
        """Retrieve chat summary for a specific topic"""
        try:
            response = self.table.get_item(Key={'topicid': topic_id})
            
            if 'Item' not in response:
                logger.warning(f"⚠️ Chat session {topic_id} not found")
                return None
            
            return response['Item'].get('chat_summary', None)
        except Exception as e:
            logger.error(f"❌ Error retrieving chat summary: {str(e)}")
            raise
    
    def save_entity_info(self, topic_id: str, entity_result: Dict[str, Any]) -> None:
        """Save entity information consistently and atomically under chat_summary.recent_info and entity_info."""
        try:
            # --- Fetch the latest item with a consistent read
            resp = self.table.get_item(Key={'topicid': topic_id}, ConsistentRead=True)
            item = resp.get('Item', {}) or {}

            if not item:
                logger.warning(f"⚠️ save_entity_info: session {topic_id} not found, creating session.")
                self.create_chat_session(topic_id=topic_id)
                resp = self.table.get_item(Key={'topicid': topic_id}, ConsistentRead=True)
                item = resp.get('Item', {}) or {}

            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

            # --- Build normalized new data
            new_data = {
                'make': entity_result.get('make', ''),
                'model': entity_result.get('model', ''),
                'equipment': entity_result.get('equipment', ''),
                'vessel': entity_result.get('vessel', ''),
                'problems': entity_result.get('problems', []),
                'info_type': entity_result.get('info_type', ''),
                'make_type': entity_result.get('make_type', ''),
                'detail': entity_result.get('detail', ''),
                'engine_type': entity_result.get('engine_type', ''),
                'last_updated': timestamp
            }

            # --- Merge intelligently with existing entity_info
            existing_info = item.get('entity_info', {}) or {}
            merged_info = existing_info.copy()

            for k, v in new_data.items():
                if v not in [None, '', []]:
                    merged_info[k] = v

            # --- Merge into chat_summary.recent_info too
            chat_summary = item.get('chat_summary', {}) or {}
            recent_info = chat_summary.get('recent_info', {}) or {}
            recent_info.update({k: v for k, v in merged_info.items() if v not in [None, '', []]})
            chat_summary['recent_info'] = recent_info
            chat_summary['last_updated'] = timestamp

            # --- Overwrite the full record atomically with put_item
            # --- Atomically update only the relevant sections instead of overwriting whole item
            self.table.update_item(
                Key={'topicid': topic_id},
                UpdateExpression="""
                    SET chat_summary.recent_info = :ri,
                        chat_summary.last_updated = :ts,
                        entity_info = :ei,
                        updated_at = :ts
                """,
                ExpressionAttributeValues={
                    ':ri': recent_info,
                    ':ei': merged_info,
                    ':ts': timestamp
                }
            )

            # --- Verify write immediately with a consistent read
            for attempt in range(3):
                check = self.table.get_item(Key={'topicid': topic_id}, ConsistentRead=True)
                confirmed = check.get('Item', {}).get('entity_info', {})
                if confirmed:
                    logger.info(f"✅ Entity info verified for topic {topic_id}: {confirmed}")
                    break
                time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Error saving entity info: {str(e)}")

    def append_pending_prompt(self, topic_id: str, prompt: str) -> None:
        ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            self.table.update_item(
                Key={'topicid': topic_id},
                UpdateExpression=(
                    "SET chat_summary.pending_prompts = "
                    "list_append(if_not_exists(chat_summary.pending_prompts, :empty), :p), "
                    "chat_summary.last_updated = :ts"
                ),
                ExpressionAttributeValues={
                    ':p': [prompt],
                    ':empty': [],
                    ':ts': ts
                }
            )
            logger.info(f"✅ Appended pending prompt for {topic_id}")
        except Exception as e:
            logger.warning(f"⚠️ append_pending_prompt failed for {topic_id}: {e}")

    def get_topics_for_entity(self, make: str, model: str, equipment: str) -> List[str]:
        """Return list of topic_ids indexed for this entity (may be empty)."""
        try:
            if not (make and model and equipment):
                return []
            key = f"ENTITY#{make.strip().lower()}#{model.strip().lower()}#{equipment.strip().lower()}"
            resp = self.table.get_item(Key={'topicid': key})
            if 'Item' in resp:
                return resp['Item'].get('topics', [])
        except Exception as e:
            logger.warning(f"⚠️ get_topics_for_entity failed: {e}")
        return []

    def get_recent_entities(self, topic_id: str, consistent: bool = False) -> Dict[str, Any]:
        """Fetch latest entity information with full context fidelity.
        Set consistent=True for immediate read-after-write accuracy.
        """
        try:
            params = {'Key': {'topicid': topic_id}, 'ProjectionExpression': 'entity_info, chat_summary', 'ConsistentRead': True}
            if consistent:
                params['ConsistentRead'] = True  # 🔥 enables strong consistency

            response = self.table.get_item(**params)

            if 'Item' not in response:
                return {}

            item = response['Item']
            chat_summary = item.get('chat_summary', {})
            entity_info = item.get('entity_info', {})
            recent_info = chat_summary.get('recent_info', {}) or {}

            merged = {
                'make': recent_info.get('make') or entity_info.get('make', ''),
                'model': recent_info.get('model') or entity_info.get('model', ''),
                'equipment': recent_info.get('equipment') or entity_info.get('equipment', ''),
                'vessel': recent_info.get('vessel', ''),
                'problems': recent_info.get('problems', []),
                'info_type': recent_info.get('info_type', ''),
                'make_type': recent_info.get('make_type', ''),
                'detail': recent_info.get('detail', ''),
                'engine_type': recent_info.get('engine_type', '')
            }

            logger.info(f"📋 Retrieved enriched entities for topic {topic_id}: {merged}")
            return merged

        except Exception as e:
            logger.error(f"❌ Error getting recent entities: {str(e)}")
            return {}
    
    def update_summary_with_mistral(self, topic_id):
        """Update chat summary using Mistral LLM"""
        try:
            # Import here to avoid circular dependency
            from llm_client import llm_client
            
            # Get current chat history
            chat_history = self.get_chat_history(topic_id)
            
            if not chat_history:
                logger.warning(f"⚠️ No chat history found for topic {topic_id}")
                return None
            
            # Format chat history for Mistral
            formatted_history = self._format_chat_history_for_llm(chat_history)
            
            # Generate summary prompt
            summary_prompt = self._generate_summary_prompt(formatted_history)
            
            # Get LLM summary
            llm_summary = llm_client.generate_claude_response(summary_prompt)
            
            # Parse and structure the summary
            structured_summary = self._parse_llm_summary(llm_summary)
            
            # Update timestamp
            timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            structured_summary['last_updated'] = timestamp
            
            # Save updated summary
            # Preserve existing context_blocks + pending_prompts before overwriting
            existing = self.table.get_item(Key={'topicid': topic_id}).get('Item', {})
            old_summary = existing.get('chat_summary', {})

            structured_summary['context_blocks'] = old_summary.get('context_blocks', [])
            structured_summary['pending_prompts'] = old_summary.get('pending_prompts', [])

            self.table.update_item(
                Key={'topicid': topic_id},
                UpdateExpression="SET chat_summary = :summary, context_blocks = :ctx, updated_at = :ua",
                ExpressionAttributeValues={
                    ':summary': structured_summary,
                    ':ctx': structured_summary['context_blocks'],
                    ':ua': timestamp
                }
            )
            
            logger.info(f"✅ Updated chat summary with Mistral LLM for session {topic_id}")
            return structured_summary
        except Exception as e:
            logger.error(f"❌ Error updating chat summary with Mistral LLM: {str(e)}")
            # Fallback to basic summary
            self._update_chat_summary_basic(topic_id, chat_history)
            return None

    def _update_chat_summary_basic(
        self,
        topic_id: str,
        original_prompt: str = None,
        equipment_context: dict = None,
        rephrased_query: str = None,
        current_entities: dict = None,
        problems: list = None,
        append_prompt: bool = True
    ):
        """
        Safely append a new message entry to chat_summary.navigation_flow and maintain context_blocks.
        Supports both older calls (with equipment_context) and newer calls that pass current_entities or problems.
        """
        try:
            # --- Normalize inputs
            equipment_context = equipment_context or current_entities or {}
            problems = problems or []
            if not isinstance(equipment_context, dict):
                logger.warning(f"⚠️ Invalid equipment_context type: {type(equipment_context)}. Skipping update.")
                return chat_summary
            now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

            # --- Fetch existing item
            resp = self.table.get_item(Key={'topicid': topic_id})
            item = resp.get('Item', {}) or {}
            chat_summary = item.get('chat_summary', {}) or {}

            # --- Ensure structures exist
            nav_flow = chat_summary.get('navigation_flow', [])
            if isinstance(nav_flow, str):
                nav_flow = [{"system_event": nav_flow, "timestamp": now}]
            elif not isinstance(nav_flow, list):
                nav_flow = []

            context_blocks = chat_summary.get('context_blocks', [])
            if not isinstance(context_blocks, list):
                context_blocks = []

            pending_prompts = chat_summary.get('pending_prompts', [])
            if not isinstance(pending_prompts, list):
                pending_prompts = []

            # --- Dedup nav_flow by exact question + context
            def _nav_entry_matches(entry, prompt, target_ctx):
                if entry.get('question') != prompt:
                    return False
                entry_ctx = entry.get('equipment_context') or {}
                tgt = target_ctx or {}
                for k, v in tgt.items():
                    if v:
                        entry_val = str(entry_ctx.get(k, '') or '').strip().lower()
                        tgt_val = str(v).strip().lower()
                        if entry_val != tgt_val:
                            return False
                return True

            if append_prompt and any(_nav_entry_matches(e, original_prompt, equipment_context) for e in nav_flow):
                # prevent re-append of same prompt within 3 seconds (session-level dedup)
                if nav_flow and nav_flow[-1].get("question") == original_prompt:
                    last_ts = datetime.datetime.strptime(nav_flow[-1]["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
                    if (datetime.datetime.now(datetime.timezone.utc) - last_ts).total_seconds() < 3:
                        logger.debug(f"⏳ Skipping rapid duplicate append for {original_prompt}")
                        return chat_summary

            if append_prompt and original_prompt:
                print(f"➡️ Appended prompt to context_block (make={equipment_context.get('make')}, model={equipment_context.get('model')}, equipment={equipment_context.get('equipment')}) for topic {topic_id}")

                new_entry = {
                    "question": original_prompt,
                    "rephrased": rephrased_query or "",
                    "equipment_context": equipment_context or {},
                    "timestamp": now
                }
                nav_flow.append(new_entry)

            # --- Helper for matching existing context blocks
            def _matches(block_ctx, target_ctx):
                if not block_ctx or not target_ctx:
                    return False
                for k, v in (target_ctx or {}).items():
                    if v:
                        bc_val = str(block_ctx.get(k, "") or "").strip().lower()
                        tgt_val = str(v or "").strip().lower()
                        if bc_val != tgt_val:
                            return False
                return True

            has_key_fields = any(equipment_context.get(k) for k in ('make', 'model', 'equipment'))

            if has_key_fields:
                matched = None
                for block in context_blocks:
                    if _matches(block.get('equipment_context', {}), equipment_context):
                        matched = block
                        break

                if matched:
                    if original_prompt and original_prompt not in matched.get('prompts', []):
                        matched.setdefault('prompts', []).append(original_prompt)
                    matched['last_updated'] = now
                else:
                    new_block = {
                        "equipment_context": equipment_context,
                        "prompts": [original_prompt] if original_prompt else [],
                        "last_updated": now
                    }
                    context_blocks.append(new_block)
            else:
                if original_prompt and original_prompt not in pending_prompts:
                    pending_prompts.append(original_prompt)

            # --- Merge recent_info (non-empty values only)
            recent_info = chat_summary.get('recent_info', {}) or {}
            for k in ('make', 'model', 'equipment', 'vessel', 'engine_type', 'make_type', 'detail', 'info_type'):
                v = equipment_context.get(k)
                if v:
                    recent_info[k] = v
            recent_info.setdefault('problems', problems or chat_summary.get('recent_info', {}).get('problems', []))

            # --- Build final summary object
            updated_summary = {
                "navigation_flow": nav_flow,
                "recent_info": recent_info,
                "context_blocks": context_blocks,
                "pending_prompts": pending_prompts,
                "last_updated": now
            }

            # --- Persist
            self.table.update_item(
                Key={'topicid': topic_id},
                UpdateExpression="SET chat_summary = :summary, updated_at = :ua",
                ExpressionAttributeValues={
                    ':summary': updated_summary,
                    ':ua': now
                }
            )

            logger.info(f"✅ Chat summary updated: nav_flow={len(nav_flow)}, context_blocks={len(context_blocks)}, pending_prompts={len(pending_prompts)} for {topic_id}")
            return updated_summary

        except Exception as e:
            logger.error(f"❌ Error updating chat summary for topic {topic_id}: {str(e)}")
            return {}

    def get_recent_chat_history(self, topic_id: str, limit: int = 2):
        """
        Returns the last N user+assistant turns for this topic_id.
        Prevents overloading the LLM with the entire session.
        """
        resp = self.table.get_item(Key={'topicid': topic_id})
        
        # ✅ Correct key — single item, not list
        item = resp.get('Item', {})
        if not item:
            return []

        # ✅ Deserialize DynamoDB types into plain Python (if needed)
        def deserialize(obj):
            if isinstance(obj, dict) and any(k in obj for k in ('S', 'N', 'L', 'M', 'BOOL', 'NULL')):
                return deserializer.deserialize(obj)
            return obj

        # If 'chat-history' exists, deserialize it
        chat_history = item.get('chat-history', [])
        chat_history = deserialize(chat_history)

        # ✅ Handle weird nested structures (list of lists, dicts, etc.)
        flat_history = []
        for entry in chat_history:
            if isinstance(entry, list):
                flat_history.extend(entry)
            else:
                flat_history.append(entry)

        # ✅ Optional: Normalize structure (so each message is a dict)
        normalized = []
        for msg in flat_history:
            if isinstance(msg, dict):
                normalized.append(msg)
            elif isinstance(msg, str):
                normalized.append({"Role": "user", "Prompt": msg})
            else:
                # Skip or log unknown types
                pass

        # ✅ Take the last `limit * 2` messages
        recent = normalized[-(limit * 5):]
        return recent

    def get_context_block_history(self, topic_id, equipment_context):
        """Return chat text only from context blocks matching current equipment (subset match + chronological pairing)."""
        try:
            resp = self.table.get_item(Key={'topicid': topic_id})
            item = resp.get('Item', {}) or {}
            summary = item.get('chat_summary', {}) or {}
            blocks = summary.get('context_blocks', [])

            if not blocks:
                logger.debug(f"ℹ️ No context_blocks present for {topic_id}")
                return ""

            # Subset matching helper
            def _matches(block_ctx, target_ctx):
                if not block_ctx or not target_ctx:
                    return False
                for k, v in (target_ctx or {}).items():
                    if v:
                        if str(block_ctx.get(k, '') or '').strip().lower() != str(v).strip().lower():
                            return False
                return True

            matches = [b for b in blocks if _matches(b.get('equipment_context', {}), equipment_context)]
            if not matches:
                logger.debug(f"ℹ️ No context block match found for {topic_id} / {equipment_context}")
                return ""

            # Build chronological history by locating matching prompts and the assistant reply right after them
            full_history = self.get_chat_history(topic_id) or []
            history_lines = []

            for block in matches:
                prompts = block.get('prompts', [])
                if not prompts:
                    continue

                for i, msg in enumerate(full_history):
                    # user messages
                    if msg.get('Role') == 'user' and msg.get('Prompt') in prompts:
                        history_lines.append(f"User: {msg.get('Prompt')}")
                        # attach assistant response immediately following (if exists)
                        if i + 1 < len(full_history) and full_history[i + 1].get('Role') == 'assistant':
                            resp_obj = full_history[i + 1].get('Response', {})
                            # handle response object / string
                            if isinstance(resp_obj, dict):
                                history_lines.append(f"Assistant: {resp_obj.get('response', '')}")
                            else:
                                history_lines.append(f"Assistant: {resp_obj}")
            return "\n".join(history_lines)
        except Exception as e:
            logger.warning(f"⚠️ get_context_block_history failed for {topic_id}: {e}")
            return ""

    def resolve_pending_contexts(self, topic_id: str, entity_result: Dict[str, Any]) -> None:
        """
        Move any pending_prompts into the appropriate context_block based on the supplied entity_result.
        Called after save_entity_info when new equipment context becomes available.
        """
        try:
            # Build equipment_context from entity_result
            equipment_context = {
                'make': entity_result.get('make', '') or '',
                'model': entity_result.get('model', '') or '',
                'equipment': entity_result.get('equipment', '') or '',
                'vessel': entity_result.get('vessel', '') or ''
            }

            # fetch current summary
            resp = self.table.get_item(Key={'topicid': topic_id})
            item = resp.get('Item', {}) or {}
            chat_summary = item.get('chat_summary', {}) or {}
            pending_prompts = chat_summary.get('pending_prompts', []) or []

            if not pending_prompts:
                logger.debug("ℹ️ No pending prompts to resolve.")
                return

            logger.info(f"🔁 Resolving {len(pending_prompts)} pending prompts into context for {topic_id} -> {equipment_context}")

            # For each pending prompt, call _update_chat_summary_basic which will handle block creation
            for prompt in pending_prompts:
                try:
                    # reuse _update_chat_summary_basic to create/append into context_block
                    self._update_chat_summary_basic(topic_id, prompt, equipment_context, rephrased_query=None)
                except Exception as ex:
                    logger.warning(f"⚠️ Failed to attach pending prompt '{prompt}': {ex}")

            # Clear pending_prompts once resolution attempts completed
            now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            chat_summary['pending_prompts'] = []
            chat_summary['last_updated'] = now

            self.table.update_item(
                Key={'topicid': topic_id},
                UpdateExpression="SET chat_summary = :summary, updated_at = :ua",
                ExpressionAttributeValues={':summary': chat_summary, ':ua': now}
            )
            logger.info(f"✅ Resolved pending prompts for topic {topic_id}")

        except Exception as e:
            logger.error(f"❌ Error in resolve_pending_contexts for {topic_id}: {e}")


    def _format_chat_history_for_llm(self, chat_history):
        """Format chat history for LLM consumption"""
        formatted_messages = []
        
        for msg in chat_history:
            if msg.get("Role") == "user":
                formatted_messages.append(f"User: {msg.get('Prompt', '')}")
            elif msg.get("Role") == "assistant":
                # Get the response content - might be HTML or text
                response_obj = msg.get("Response", {})
                response_content = response_obj.get("response", "")
                response_type = response_obj.get("response_type", "Text")
                
                # If it's HTML, extract just the text content to avoid HTML tags
                if response_type and response_type.upper().startswith("HTML"):
                    response_text = self._extract_text_from_html(response_content)
                    formatted_messages.append(f"Assistant: {response_text}")
                else:
                    formatted_messages.append(f"Assistant: {response_content}")
        
        return "\n".join(formatted_messages)
    
    def _extract_text_from_html(self, html_content):
        """Very basic HTML text extraction"""
        import re
        
        # Basic removal of common HTML tags for summary purposes
        text = html_content
        
        # Remove doctype, html, head, and script tags with content
        for tag in ['<!DOCTYPE[^>]*>', '<html[^>]*>.*?</html>', '<head>.*?</head>', '<script>.*?</script>']:
            text = re.sub(tag, '', text, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove other common tags
        text = re.sub(r'<[^>]*>', ' ', text)
        text = re.sub(r'\n|\t', ' ', text)
        
        # Normalize spaces
        while '  ' in text:
            text = text.replace('  ', ' ')
        
        return text.strip()
    
    def _generate_summary_prompt(self, formatted_history):
        """Generate prompt for summary generation"""
        return f"""You're a Maritime domain expert performing conversation analysis.
Analyze this conversation between a user and maritime assistant, then extract:

1. Navigation flow - summarize the conversation flow very concisely showing how topics evolved.
2. Key information mentioned:
   - Engine/equipment make (e.g., Caterpillar, Wärtsilä)
   - Engine/equipment model (e.g., 3516, RT-flex96C)
   - Vessel mentioned (if any)
   - Equipment type (engine, propeller, generator, etc.)
   - Problems or maintenance procedures discussed

Use the following JSON format for your response:
{{
  "navigation_flow": "concise description of conversation flow",
  "recent_info": {{
    "make": "equipment manufacturer",
    "model": "model number/name",
    "vessel": "vessel name/type if mentioned",
    "equipment": "type of equipment discussed",
    "problems": ["list", "of", "issues", "discussed"]
  }}
}}

Conversation:
{formatted_history}

Extract only the key information mentioned in the conversation. If something wasn't mentioned, set it to null. Respond ONLY with the JSON - no other text.
"""

    def _parse_llm_summary(self, llm_summary):
        """Parse LLM-generated summary into structured format"""
        try:
            # Try to parse as JSON
            summary_dict = json.loads(llm_summary)
            
            # Ensure required fields exist
            if "navigation_flow" not in summary_dict:
                summary_dict["navigation_flow"] = "Conversation about maritime equipment"
            
            if "recent_info" not in summary_dict:
                summary_dict["recent_info"] = {
                    "make": None,
                    "model": None,
                    "vessel": None,
                    "equipment": None,
                    "problems": []
                }
            else:
                # Ensure all fields exist in recent_info
                for field in ["make", "model", "vessel", "equipment"]:
                    if field not in summary_dict["recent_info"]:
                        summary_dict["recent_info"][field] = None
                
                if "problems" not in summary_dict["recent_info"]:
                    summary_dict["recent_info"]["problems"] = []
            
            return summary_dict
        except Exception as e:
            logger.error(f"❌ Error parsing LLM summary: {str(e)}")
            # Return default structure
            return {
                "navigation_flow": "Conversation about maritime equipment",
                "recent_info": {
                    "make": None,
                    "model": None,
                    "vessel": None,
                    "equipment": None,
                    "problems": []
                }
            }
    
    def delete_chat_session(self, topic_id):
        """Delete a chat session"""
        try:
            self.table.delete_item(Key={'topicid': topic_id})
            logger.info(f"✅ Deleted chat session {topic_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Error deleting chat session: {str(e)}")
            raise

    def store_final_response(self, record: Dict[str, Any]) -> None:
        """
        Store final/partial response record for disconnected sessions or fallbacks.
        This appends the final response into the session's 'final_responses' list
        to avoid overwriting the session item (table uses topicid as PK).
        """
        try:
            topic_id = record.get("topicid") or record.get("topic_id")
            if not topic_id:
                logger.warning("⚠️ store_final_response called without topic id")
                return

            ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            final_record = {
                'final_record_id': f"FINAL#{record.get('transaction_id', str(uuid.uuid4()))}#{int(time.time())}",
                'final_response': record.get('final_response', {}),
                'status': record.get('status', 'completed'),
                'stored_at': record.get('timestamp', ts),
                'ttl': record.get('ttl', int(time.time()) + 3600)
            }

            # Atomically append to a list attribute 'final_responses' inside the session item
            self.table.update_item(
                Key={'topicid': topic_id},
                UpdateExpression="SET final_responses = list_append(if_not_exists(final_responses, :empty_list), :fr), updated_at = :ua",
                ExpressionAttributeValues={
                    ':fr': [final_record],
                    ':empty_list': [],
                    ':ua': ts
                }
            )
            logger.info(f"💾 Stored final response for topic {topic_id}")
        except Exception as e:
            logger.error(f"❌ Error storing final response: {str(e)}")

    def save_processing_status(self, topic_id: str, status: str, progress: int = 0, details: Optional[str] = None) -> None:
        """
        Save ephemeral processing status with a short TTL for reconnection scenarios.
        Appends status into session.chat_summary.processing_status_history and updates a compact field.
        """
        try:
            ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            status_entry = {
                'processing_status_id': f"STATUS#{int(time.time())}#{str(uuid.uuid4())}",
                'status': status,
                'progress': progress,
                'details': details or "",
                'timestamp': ts,
                'ttl': int(time.time()) + 7200  # 2 hours
            }

            # Append to a 'processing_status_history' list inside the session item atomically
            self.table.update_item(
                Key={'topicid': topic_id},
                UpdateExpression="SET processing_status_history = list_append(if_not_exists(processing_status_history, :empty_list), :ps), chat_summary.processing_status = :ps_compact, updated_at = :ua",
                ExpressionAttributeValues={
                    ':ps': [status_entry],
                    ':empty_list': [],
                    ':ps_compact': {'status': status, 'progress': progress, 'details': details or "", 'timestamp': ts},
                    ':ua': ts
                }
            )

            logger.info(f"💾 Saved processing status for topic {topic_id}: {status} ({progress}%)")
        except Exception as e:
            logger.error(f"❌ Error saving processing status: {str(e)}")

# Global DynamoDB manager instance
dynamo_manager = DynamoDBChatManager()