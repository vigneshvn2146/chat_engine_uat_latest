"""
Enhanced DynamoDB Chat History Module with Session Management
Adds session listing and management capabilities for Streamlit interface
"""

import boto3
import json
import uuid
import datetime
import logging
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from config import config
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd

logger = logging.getLogger(__name__)

class EnhancedDynamoDBChatManager:
    """
    Enhanced version of DynamoDBChatManager with session management capabilities
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
                        {'AttributeName': 'Key topicid', 'KeyType': 'HASH'}  # Primary key with correct name
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'Key topicid', 'AttributeType': 'S'}
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
    
    def list_all_sessions(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        List all chat sessions with basic information
        
        Args:
            limit: Maximum number of sessions to return
            
        Returns:
            List of session dictionaries with metadata
        """
        try:
            logger.info(f"📋 Scanning for sessions (limit: {limit})")
            
            response = self.table.scan(
                ProjectionExpression='#topicid, created_at, updated_at, chat_summary, topic_name',
                ExpressionAttributeNames={
                    '#topicid': 'Key topicid'
                },
                Limit=limit
            )
            
            sessions = []
            for item in response.get('Items', []):
                session_info = {
                    'session_id': item.get('Key topicid', ''),
                    'created_at': item.get('created_at', ''),
                    'updated_at': item.get('updated_at', ''),
                    'topic_name': item.get('topic_name', ''),
                    'last_activity': item.get('updated_at', item.get('created_at', '')),
                }
                
                # Add summary info if available
                chat_summary = item.get('chat_summary', {})
                if chat_summary:
                    navigation_flow = chat_summary.get('navigation_flow', '')
                    if navigation_flow:
                        # Take first part of navigation flow as preview
                        preview = navigation_flow[:100] + "..." if len(navigation_flow) > 100 else navigation_flow
                        session_info['preview'] = preview
                    
                    recent_info = chat_summary.get('recent_info', {})
                    if recent_info:
                        # Create equipment summary
                        equipment_parts = []
                        if recent_info.get('make'):
                            equipment_parts.append(recent_info['make'])
                        if recent_info.get('model'):
                            equipment_parts.append(recent_info['model'])
                        if recent_info.get('equipment'):
                            equipment_parts.append(recent_info['equipment'])
                        
                        if equipment_parts:
                            session_info['equipment'] = " ".join(equipment_parts)
                
                sessions.append(session_info)
            
            # Sort by last activity (most recent first)
            sessions.sort(key=lambda x: x.get('last_activity', ''), reverse=True)
            
            logger.info(f"✅ Found {len(sessions)} sessions")
            return sessions
            
        except Exception as e:
            logger.error(f"❌ Error listing sessions: {str(e)}")
            return []
    
    def get_session_summary(self, session_id: str) -> Dict[str, Any]:
        """
        Get detailed summary for a specific session
        
        Args:
            session_id: The session ID to get summary for
            
        Returns:
            Dictionary with session summary information
        """
        try:
            response = self.table.get_item(
                Key={'Key topicid': session_id},
                ProjectionExpression='#topicid, created_at, updated_at, chat_summary, entity_info, #ch',
                ExpressionAttributeNames={
                    '#topicid': 'Key topicid',
                    '#ch': 'chat-history'
                }
            )
            
            if 'Item' not in response:
                return {}
            
            item = response['Item']
            chat_history = item.get('chat-history', [])
            
            summary = {
                'session_id': session_id,
                'created_at': item.get('created_at', ''),
                'updated_at': item.get('updated_at', ''),
                'message_count': len(chat_history),
                'chat_summary': item.get('chat_summary', {}),
                'entity_info': item.get('entity_info', {}),
            }
            
            # Count user vs assistant messages
            user_messages = len([msg for msg in chat_history if msg.get('Role') == 'user'])
            assistant_messages = len([msg for msg in chat_history if msg.get('Role') == 'assistant'])
            
            summary['user_messages'] = user_messages
            summary['assistant_messages'] = assistant_messages
            
            # Get first and last message timestamps
            if chat_history:
                timestamps = []
                for msg in chat_history:
                    if msg.get('Prompt_timestamp'):
                        timestamps.append(msg['Prompt_timestamp'])
                    if msg.get('Response_timestamp'):
                        timestamps.append(msg['Response_timestamp'])
                
                if timestamps:
                    summary['first_message'] = min(timestamps)
                    summary['last_message'] = max(timestamps)
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Error getting session summary: {str(e)}")
            return {}
    
    def search_sessions(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Search sessions by content or equipment information
        
        Args:
            query: Search query string
            limit: Maximum number of sessions to return
            
        Returns:
            List of matching sessions
        """
        try:
            logger.info(f"🔍 Searching sessions for: '{query}'")
            
            # Get all sessions first (you might want to optimize this with better indexing)
            all_sessions = self.list_all_sessions(limit=100)
            
            # Filter sessions based on query
            matching_sessions = []
            query_lower = query.lower()
            
            for session in all_sessions:
                match_score = 0
                
                # Check session preview
                if 'preview' in session and query_lower in session['preview'].lower():
                    match_score += 3
                
                # Check equipment info
                if 'equipment' in session and query_lower in session['equipment'].lower():
                    match_score += 2
                
                # Check session ID (partial match)
                if query_lower in session['session_id'].lower():
                    match_score += 1
                
                if match_score > 0:
                    session['match_score'] = match_score
                    matching_sessions.append(session)
            
            # Sort by match score and then by last activity
            matching_sessions.sort(key=lambda x: (x['match_score'], x.get('last_activity', '')), reverse=True)
            
            logger.info(f"✅ Found {len(matching_sessions)} matching sessions")
            return matching_sessions[:limit]
            
        except Exception as e:
            logger.error(f"❌ Error searching sessions: {str(e)}")
            return []
    
    def get_session_statistics(self) -> Dict[str, Any]:
        """
        Get overall statistics about all sessions
        
        Returns:
            Dictionary with session statistics
        """
        try:
            logger.info("📊 Computing session statistics")
            
            # Get basic session count
            response = self.table.scan(
                Select='COUNT'
            )
            total_sessions = response.get('Count', 0)
            
            # Get sessions for detailed stats
            sessions = self.list_all_sessions(limit=100)
            
            # Compute statistics
            stats = {
                'total_sessions': total_sessions,
                'recent_sessions': len(sessions),
                'sessions_with_equipment': 0,
                'sessions_with_summary': 0,
                'average_messages_per_session': 0,
                'most_common_equipment': {},
                'activity_by_date': {}
            }
            
            if not sessions:
                return stats
            
            total_messages = 0
            equipment_counts = {}
            
            for session in sessions:
                # Count sessions with equipment info
                if session.get('equipment'):
                    stats['sessions_with_equipment'] += 1
                    equipment = session['equipment']
                    equipment_counts[equipment] = equipment_counts.get(equipment, 0) + 1
                
                # Count sessions with summary
                if session.get('preview'):
                    stats['sessions_with_summary'] += 1
                
                # Activity by date
                last_activity = session.get('last_activity', '')
                if last_activity:
                    date = last_activity[:10]  # Extract date part
                    stats['activity_by_date'][date] = stats['activity_by_date'].get(date, 0) + 1
            
            # Most common equipment
            if equipment_counts:
                sorted_equipment = sorted(equipment_counts.items(), key=lambda x: x[1], reverse=True)
                stats['most_common_equipment'] = dict(sorted_equipment[:5])
            
            logger.info(f"✅ Statistics computed for {total_sessions} sessions")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error computing statistics: {str(e)}")
            return {
                'total_sessions': 0,
                'recent_sessions': 0,
                'sessions_with_equipment': 0,
                'sessions_with_summary': 0,
                'average_messages_per_session': 0,
                'most_common_equipment': {},
                'activity_by_date': {}
            }
    
    # Include all existing methods from the original DynamoDBChatManager
    def create_chat_session(self, topic_id=None, topic_name=None, user_id=None):
        """Create a new chat session"""
        if not topic_id:
            topic_id = str(uuid.uuid4())
            
        # Initial session data
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        item = {
            'Key topicid': topic_id,  # Using the correct key name with space
            'chat-history': [],
            'chat_summary': {
                'navigation_flow': "Session initiated",
                'recent_info': {
                    'make': None,
                    'model': None,
                    'vessel': None,
                    'equipment': None,
                    'problems': []
                },
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
        
        # Add optional fields if provided
        if user_id:
            item['user_id'] = user_id
        if topic_name:
            item['topic_name'] = topic_name
        
        # Save to DynamoDB
        try:
            self.table.put_item(Item=item)
            logger.info(f"✅ Created new chat session with topic-id: {topic_id}")
            return topic_id
        except Exception as e:
            logger.error(f"❌ Error creating chat session: {str(e)}")
            raise
    
    def add_message(self, topic_id, message_data, update_summary=True):
        """Add a new message to an existing chat session"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        try:
            # Get current chat history
            response = self.table.get_item(Key={'Key topicid': topic_id})
            
            if 'Item' not in response:
                logger.warning(f"⚠️ Chat session {topic_id} not found, creating new session")
                self.create_chat_session(topic_id=topic_id)
                chat_history = []
            else:
                chat_history = response['Item'].get('chat-history', [])
            
            # Add new message to chat history
            chat_history.append(message_data)
            
            # Update item in DynamoDB
            self.table.update_item(
                Key={'Key topicid': topic_id},
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
            
            # Update the chat summary if requested
            if update_summary and message_data.get("Role") == "assistant":
                self.update_summary_with_mistral(topic_id)
            
            logger.info(f"✅ Added message to chat session {topic_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Error adding message to chat session: {str(e)}")
            raise
    
    def get_chat_history(self, topic_id):
        """Retrieve chat history for a specific topic"""
        try:
            response = self.table.get_item(Key={'Key topicid': topic_id})
            
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
            response = self.table.get_item(Key={'Key topicid': topic_id})
            
            if 'Item' not in response:
                logger.warning(f"⚠️ Chat session {topic_id} not found")
                return None
            
            return response['Item'].get('chat_summary', None)
        except Exception as e:
            logger.error(f"❌ Error retrieving chat summary: {str(e)}")
            raise
    
    def save_entity_info(self, topic_id: str, entity_result: Dict[str, Any]) -> None:
        """Save entity information to DynamoDB"""
        try:
            response = self.table.get_item(Key={'Key topicid': topic_id})
            
            if 'Item' in response:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                entity_info = {
                    'make': entity_result.get('make', ''),
                    'model': entity_result.get('model', ''),
                    'equipment': entity_result.get('equipment', ''),
                    'last_updated': timestamp
                }
                
                self.table.update_item(
                    Key={'Key topicid': topic_id},
                    UpdateExpression="SET entity_info = :entity_info, updated_at = :updated_at",
                    ExpressionAttributeValues={
                        ':entity_info': entity_info,
                        ':updated_at': timestamp
                    }
                )
                
                logger.info(f"✅ Updated entity info for topic {topic_id}")
        except Exception as e:
            logger.error(f"❌ Error saving entity info: {str(e)}")
    
    def get_recent_entities(self, topic_id: str) -> Dict[str, Any]:
        """Get recent entity information from session"""
        try:
            response = self.table.get_item(
                Key={'Key topicid': topic_id},
                ProjectionExpression='entity_info, chat_summary'
            )
            
            if 'Item' in response:
                entity_info = response['Item'].get('entity_info', {})
                chat_summary = response['Item'].get('chat_summary', {})
                
                recent_entities = {
                    'make': entity_info.get('make') or chat_summary.get('recent_info', {}).get('make'),
                    'model': entity_info.get('model') or chat_summary.get('recent_info', {}).get('model'),
                    'equipment': entity_info.get('equipment') or chat_summary.get('recent_info', {}).get('equipment'),
                    'vessel': chat_summary.get('recent_info', {}).get('vessel'),
                    'problems': chat_summary.get('recent_info', {}).get('problems', [])
                }
                
                logger.info(f"📋 Retrieved entities for topic {topic_id}")
                return recent_entities
            
            return {}
        except Exception as e:
            logger.error(f"❌ Error getting recent entities: {str(e)}")
            return {}
    
    def delete_chat_session(self, topic_id):
        """Delete a chat session"""
        try:
            self.table.delete_item(Key={'Key topicid': topic_id})
            logger.info(f"✅ Deleted chat session {topic_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Error deleting chat session: {str(e)}")
            raise
    
    def update_summary_with_mistral(self, topic_id):
        """Update chat summary using Mistral LLM - simplified version"""
        try:
            # Basic implementation - you can expand this based on your original implementation
            logger.info(f"📝 Updating summary for topic {topic_id}")
            # Implementation would go here based on your original code
            return True
        except Exception as e:
            logger.error(f"❌ Error updating summary: {str(e)}")
            return False

# Create enhanced global instance
enhanced_dynamo_manager = EnhancedDynamoDBChatManager()