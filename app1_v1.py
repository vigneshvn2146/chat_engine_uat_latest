"""
Maritime RAG Application Main Orchestrator - WITH TOPIC NAME GENERATION
Implements the complete flow with proper session management and topic name generation
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
from typing import Dict, List, Optional, Tuple, Any

# Import all modules (CLEAN IMPORTS)
from config import config
from llm_client import llm_client
from dynamodb import dynamo_manager
from vespa_client import vespa_client
from entity_extraction import entity_extractor

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
            error_msg = str(e)
            if "Invalid connectionId" in error_msg:
                logger.warning(f"⚠️ Connection {connection_id} is no longer valid")
            else:
                logger.error(f"❌ WebSocket error: {error_msg}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error sending WebSocket message: {str(e)}")
            return False

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
# TOPIC NAME GENERATOR
# =============================================================================

class TopicNameGenerator:
    """Handles topic name generation using LLM"""
    
    @staticmethod
    def generate_topic_name(user_prompt: str) -> str:
        """Generate a topic name for the user query using LLM"""
        try:
            print(f"🏷️ TOPIC_GENERATION: Generating topic name for query")
            
            # Use LLM to generate a concise topic name
            topic_prompt = f"""
            Generate a short, descriptive topic name (maximum 5-6 words) for the following marine equipment query. 
            The topic should be concise and capture the main subject of the question.
            
            User Query: {user_prompt}
            
            Response format: Just return the topic name, nothing else.
            Examples:
            - "Engine Troubleshooting"
            - "Pump Maintenance Guide"
            - "Electrical System Issues"
            - "Safety Equipment Inspection"
            """
            
            topic_name = llm_client.generate_mistral_response(topic_prompt)
            
            # Clean up the topic name (remove quotes, extra whitespace, etc.)
            topic_name = topic_name.strip().strip('"').strip("'")
            
            # Ensure it's not too long
            if len(topic_name) > 50:
                topic_name = topic_name[:47] + "..."
            
            print(f"✅ TOPIC_GENERATED: {topic_name}")
            return topic_name
            
        except Exception as e:
            logger.error(f"❌ Error generating topic name: {str(e)}")
            # Fallback to a generic topic name
            return "Marine Equipment Query"

# =============================================================================
# MAIN APPLICATION CLASS
# =============================================================================

class MaritimeRAGApplication:
    """Main application orchestrator with flowchart-compliant entity handling"""
    
    def __init__(self):
        """Initialize all application components"""
        logger.info("🚀 Initializing Maritime RAG Application...")
        
        # Initialize services
        self.websocket_service = WebSocketService()
        self.response_formatter = ResponseFormatter()
        self.topic_generator = TopicNameGenerator()
        
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
    # Response Handling (UPDATED WITH TOPIC NAME SUPPORT)
    # -------------------------------------------------------------------------
    
    def send_websocket_response(self, connection_id: str, transaction_id: str, 
                              user_prompt: str, topic_id: str, prompt_timestamp: str,
                              response_data: Dict[str, Any], topic_name: Optional[str] = None) -> str:
        """Send response via WebSocket and save to DynamoDB - Returns the topic_id used"""
        response_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Validate response format
        if not self.response_formatter.validate_response_format(response_data):
            logger.warning("⚠️ Response format validation failed, but proceeding...")
        
        print(f"📤 FINAL_RESPONSE_FORMAT: {response_data}")
        
        # Save assistant message to DynamoDB
        assistant_message = {
            "transaction_id": transaction_id,
            "Prompt": user_prompt,
            "TopicID": topic_id,
            "Prompt_timestamp": prompt_timestamp,
            "Response_timestamp": response_timestamp,
            "Role": "assistant",
            "Response": response_data
        }
        
        dynamo_manager.add_message(topic_id, assistant_message)
        logger.info(f"💾 Saved response to chat history for topic {topic_id}")
        
        # Prepare WebSocket message
        websocket_message = {
            "transaction_id": transaction_id,
            "Prompt": user_prompt,
            "TopicID": topic_id,
            "Prompt_timestamp": prompt_timestamp,
            "Response_timestamp": response_timestamp,
            "Response": response_data
        }
        
        # Add topic name if this is a new topic
        if topic_name:
            websocket_message["TopicName"] = topic_name
            print(f"🏷️ TOPIC_NAME_INCLUDED: {topic_name}")
        
        print(f"📤 FINAL_WEBSOCKET_MESSAGE: {websocket_message}")
        
        self.websocket_service.send_message(connection_id, json.dumps(websocket_message))
        logger.info(f"📤 Sent response via WebSocket for topic {topic_id}")
        
        return topic_id
    
    # -------------------------------------------------------------------------
    # NEW SESSION FLOW (FLOWCHART COMPLIANT WITH TOPIC NAME)
    # -------------------------------------------------------------------------
    
    def handle_new_session(self, connection_id: str, transaction_id: str, 
                          user_prompt: str, topic_id: str, prompt_timestamp: str,
                          is_new_topic: bool = False) -> Dict[str, Any]:
        """Handle new session flow with FLOWCHART-COMPLIANT entity extraction and topic generation"""
        print(f"🆕 NEW_SESSION: Starting entity extraction for query")
        print(f"📝 USER_QUERY: {user_prompt}")
        print(f"🏷️ IS_NEW_TOPIC: {is_new_topic}")
        
        # Generate topic name if this is a new topic
        topic_name = None
        if is_new_topic:
            topic_name = self.topic_generator.generate_topic_name(user_prompt)
            print(f"🏷️ GENERATED_TOPIC_NAME: {topic_name}")
        
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
                connection_id, transaction_id, user_prompt, topic_id,
                prompt_timestamp, equipment_context, user_prompt, topic_name
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
                prompt_timestamp, response_data, topic_name
            )
            
            lambda_response = {
                "statusCode": 200, 
                "body": json.dumps({
                    "message": "OK",
                    "topic_id": final_topic_id,
                    "topic_name": topic_name if topic_name else None
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
            prompt_timestamp, equipment_context, rephrased_query
        )
    
    # -------------------------------------------------------------------------
    # MAIN PROCESSING FLOW (UPDATED WITH TOPIC NAME SUPPORT)
    # -------------------------------------------------------------------------
    
    def execute_main_processing_flow(self, connection_id: str, transaction_id: str,
                               user_prompt: str, topic_id: str, prompt_timestamp: str,
                               equipment_context: Dict[str, str], rephrased_query: str,
                               topic_name: Optional[str] = None) -> Dict[str, Any]:
        """Execute main RAG processing flow with equipment awareness and 3-tier Vespa search"""
        print(f"⚙️ MAIN_PROCESSING: Starting equipment-aware RAG pipeline")
        
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
                
                # Provide helpful no-results message based on available context
                available_context = {k: v for k, v in equipment_context.items() if v and k != 'problems'}
                available_info = [k for k, v in available_context.items() if v]
                missing_info = [k for k in ['make', 'model', 'equipment', 'vessel'] if k not in available_info]
                
                if available_info:
                    no_results_message = f"We found no relevant information for your query using the available {', '.join(available_info)} information."
                    if missing_info:
                        no_results_message += f" To get better results, consider providing {', '.join(missing_info[:2])} details."
                else:
                    no_results_message = "We found no relevant information for your query. Please provide more specific equipment details (manufacturer, model, or equipment type) for better results."
                
                response_data = self.response_formatter.create_standard_response(
                    response_text=no_results_message,
                    response_type="Text",
                    data_json=[]
                )
                
                final_topic_id = self.send_websocket_response(
                    connection_id, transaction_id, user_prompt, topic_id,
                    prompt_timestamp, response_data, topic_name
                )

                lambda_response = {
                    "statusCode": 200, 
                    "body": json.dumps({
                        "message": "No results found",
                        "topic_id": final_topic_id,
                        "topic_name": topic_name if topic_name else None
                    })
                }
                print(f"🔚 LAMBDA_RESPONSE: {lambda_response}")
                
                return lambda_response
            
            # Step 5: Prepare context from search results
            print("📋 Step 5: Preparing context from search results...")
            context = vespa_client.prepare_context(search_results)
            print(f"📏 Context length: {len(context)} characters")
            
            # Step 6: Generate RAG response with equipment awareness
            print("🤖 Step 6: Generating equipment-aware RAG response...")
            rag_response = llm_client.generate_rag_response(
                rephrased_query + "\n" + f"YQL Query : for the below results. use this query to understand the responses retreived from the databse and limitation of the search space and filter applied inorder to get the results : {yql}", 
                context, equipment_context
            )
            print(f"📝 AUGMENTED_RESPONSE: {rag_response[:]}...")
            
            # Step 7: Format response as HTML with equipment context notices
            print("🎨 Step 7: Formatting response as HTML...")
            
            # Check for missing entities to include in response
            missing_entities = []
            if not equipment_context.get('make') and not equipment_context.get('model'):
                missing_entities.extend(["make", "model"])
            elif equipment_context.get('make') and not equipment_context.get('model'):
                missing_entities.append("model")
            elif equipment_context.get('model') and not equipment_context.get('make'):
                missing_entities.append("make")
            
            final_response = llm_client.format_response_as_html(
                rag_response, rephrased_query, missing_entities
            )
            
            print(f"🎨 FINAL_RESPONSE_FORMAT: Response formatted successfully")

            # Step 8: Send response
            print("📤 Step 8: Sending final response...")
            final_topic_id = self.send_websocket_response(
                connection_id, transaction_id, user_prompt, topic_id,
                prompt_timestamp, final_response, topic_name
            )
            
            print("✅ RAG response pipeline completed successfully")
            
            lambda_response = {
                "statusCode": 200, 
                "body": json.dumps({
                    "message": "RAG response delivered",
                    "topic_id": final_topic_id,
                    "topic_name": topic_name if topic_name else None
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
                prompt_timestamp, error_response, topic_name
            )
            
            lambda_response = {
                "statusCode": 500, 
                "body": json.dumps({
                    "message": f"Processing error: {str(e)}",
                    "topic_id": final_topic_id,
                    "topic_name": topic_name if topic_name else None
                })
            }
            print(f"🔚 LAMBDA_RESPONSE: {lambda_response}")
            
            return lambda_response
    
    # -------------------------------------------------------------------------
    # Main Lambda Handler (UPDATED WITH TOPIC GENERATION)
    # -------------------------------------------------------------------------
    
    def lambda_handler(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Main AWS Lambda handler following the flowchart logic with topic generation"""
        try:
            print("🚀 Maritime RAG Lambda Handler Started")
            print("=" * 80)
            
            os.environ['INSTANCE'] = os.getenv('INSTANCE')

            print(f"Event : {event}")
            
            # Parse WebSocket event
            connection_id = event["requestContext"]["connectionId"]

            # if os.environ['TEST_MODE'] == True:
            # body = json.loads(event["body"])
            body = event["body"]
            topic_id = body.get("TopicID", "")
            user_prompt = body.get("Prompt", "")
            is_new_topic = body.get("IsNewTopic", False)  # NEW: Check for IsNewTopic flag
            
            print(f"📋 SESSION_ID: {topic_id}")
            print(f"📝 USER_QUERY: {user_prompt}")
            print(f"🏷️ IS_NEW_TOPIC: {is_new_topic}")
            
            # Generate transaction metadata
            transaction_id = str(uuid.uuid4())
            prompt_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # FLOWCHART DECISION POINT: Session Type Check
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
                
                # Handle new session flow (Flowchart-Compliant Entity Extraction)
                result = self.handle_new_session(
                    connection_id, transaction_id, user_prompt, topic_id, 
                    prompt_timestamp, is_new_topic
                )
                
                # Ensure topic_id is in response for session continuity
                if result.get("statusCode") == 200:
                    body_data = json.loads(result["body"])
                    body_data["topic_id"] = topic_id
                    result["body"] = json.dumps(body_data)
                
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
                
                # Handle existing session flow (Context + Rephrasing)
                result = self.handle_existing_session(
                    connection_id, transaction_id, user_prompt, topic_id, prompt_timestamp
                )
                
                # Ensure topic_id is in response for session continuity
                if result.get("statusCode") == 200:
                    body_data = json.loads(result["body"])
                    body_data["topic_id"] = topic_id
                    result["body"] = json.dumps(body_data)
                
                return result
                
        except Exception as e:
            logger.error(f"❌ Critical error in lambda_handler: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Send error response if possible
            try:
                if 'connection_id' in locals():
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
                        "Response": error_response_data
                    }
                    
                    self.websocket_service.send_message(connection_id, json.dumps(error_message))
            except Exception as ws_error:
                logger.error(f"❌ Failed to send error via WebSocket: {str(ws_error)}")
            
            lambda_response = {"statusCode": 500, "body": json.dumps({"message": f"Critical error: {str(e)}"})}
            print(f"🔚 LAMBDA_RESPONSE: {lambda_response}")
            
            return lambda_response

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
    print("🎯 Maritime RAG Application - With Topic Name Generation")
    print("This version includes topic name generation for new topics")
    print("All responses follow the standardized template format")
    print("Session management and flowchart compliance are maintained")
    print("=" * 80)