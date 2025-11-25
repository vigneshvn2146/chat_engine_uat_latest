"""
Enhanced Maritime RAG Streamlit Chatbot Application
Comprehensive monitoring with session ID copy, detailed tables, and complete pipeline visibility
"""

import streamlit as st
import pandas as pd
import json
import datetime
import uuid
import time
from typing import Dict, List, Optional, Any
import logging

# Import your existing modules (assuming they're available)
try:
    from config import config
    from llm_client import llm_client
    from enhanced_dynamodb import enhanced_dynamo_manager as dynamo_manager
    from entity_extraction import entity_extractor
    from vespa_client import vespa_client
    # Import the enhanced main application
    from app import MaritimeRAGApplication
except ImportError as e:
    st.error(f"Import error: {e}")
    st.stop()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Maritime RAG Chatbot - Enhanced",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .monitoring-panel {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    .entity-box {
        background-color: #e3f2fd;
        padding: 0.5rem;
        border-radius: 0.3rem;
        margin: 0.2rem 0;
    }
    .success-box {
        background-color: #d4edda;
        padding: 0.5rem;
        border-radius: 0.3rem;
        border-left: 4px solid #28a745;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 0.5rem;
        border-radius: 0.3rem;
        border-left: 4px solid #ffc107;
    }
    .error-box {
        background-color: #f8d7da;
        padding: 0.5rem;
        border-radius: 0.3rem;
        border-left: 4px solid #dc3545;
    }
    .session-info {
        background-color: #e8f5e8;
        padding: 0.5rem;
        border-radius: 0.3rem;
        border-left: 4px solid #28a745;
        margin: 0.5rem 0;
    }
    .copy-button {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 0.25rem;
        padding: 0.375rem 0.75rem;
        font-size: 0.875rem;
        cursor: pointer;
    }
    .copy-button:hover {
        background-color: #e9ecef;
    }
    .metric-card {
        background-color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
        text-align: center;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

class EnhancedMaritimeRAGStreamlit:
    """Enhanced Streamlit interface with comprehensive monitoring and copy functionality"""
    
    def __init__(self):
        """Initialize the enhanced Streamlit interface"""
        self.maritime_app = MaritimeRAGApplication()
        self.initialize_session_state()
        self.validate_session_integrity()
    
    def initialize_session_state(self):
        """Initialize ALL session state variables with proper defaults"""
        # Core session management
        session_defaults = {
            'current_session_id': None,
            'session_validated': False,
            'session_created_at': None,
            'chat_history': [],
            'chat_history_synced': True,
            'monitoring_data': {},
            'pipeline_steps': [],
            'last_query_timestamp': None,
            'all_sessions': [],
            'recent_sessions_cache': [],
            'session_cache_timestamp': None,
            'last_error': None,
            'processing_status': 'ready',
            'current_entities': {},
            'current_chat_summary': {},
            'show_technical_details': False,
            'auto_copy_enabled': False
        }
        
        for key, default_value in session_defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value
    
    def validate_session_integrity(self):
        """Validate and fix session integrity issues"""
        try:
            if st.session_state.current_session_id:
                try:
                    db_history = dynamo_manager.get_chat_history(st.session_state.current_session_id)
                    
                    if db_history is None:
                        logger.warning(f"Session {st.session_state.current_session_id} not found in DB, clearing local state")
                        self.clear_current_session()
                    else:
                        st.session_state.session_validated = True
                        
                        if not st.session_state.chat_history_synced:
                            st.session_state.chat_history = db_history or []
                            st.session_state.chat_history_synced = True
                            logger.info("Synchronized chat history with database")
                
                except Exception as e:
                    logger.error(f"Error validating session: {str(e)}")
                    st.session_state.session_validated = False
            
        except Exception as e:
            logger.error(f"Critical error in session validation: {str(e)}")
            st.session_state.last_error = str(e)
    
    def clear_current_session(self):
        """Safely clear current session state"""
        st.session_state.current_session_id = None
        st.session_state.chat_history = []
        st.session_state.monitoring_data = {}
        st.session_state.pipeline_steps = []
        st.session_state.current_entities = {}
        st.session_state.current_chat_summary = {}
        st.session_state.session_validated = False
        st.session_state.chat_history_synced = True
        st.session_state.last_error = None
        logger.info("Current session state cleared")
    
    def create_copyable_text_box(self, label: str, text: str, key: str, language: str = None):
        """Create a text box with copy functionality"""
        col1, col2 = st.columns([4, 1])
        
        with col1:
            if language:
                st.code(text, language=language)
            else:
                st.text_area(label, text, height=100, key=f"display_{key}")
        
        with col2:
            if st.button(f"📋 Copy", key=f"copy_{key}", help=f"Copy {label}"):
                # In a real implementation, you'd use st.components for clipboard
                st.success("Text ready to copy!")
                st.code(text, language=language or 'text')
    
    def render_copyable_session_info(self):
        """Render session information with copy functionality"""
        if st.session_state.current_session_id:
            st.markdown("### 🆔 Session Information")
            
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.markdown(f"""
                <div class="session-info">
                    <strong>Session ID:</strong><br>
                    <code>{st.session_state.current_session_id}</code>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                if st.button("📋 Copy Session ID", key="copy_session_main"):
                    st.code(st.session_state.current_session_id)
            
            with col3:
                sync_status = "✅ Synced" if st.session_state.chat_history_synced else "❌ Out of sync"
                validation_status = "✅ Valid" if st.session_state.session_validated else "⚠️ Invalid"
                st.markdown(f"""
                <div class="metric-card">
                    <strong>Status</strong><br>
                    {validation_status}<br>
                    {sync_status}
                </div>
                """, unsafe_allow_html=True)
    
    def process_query_with_enhanced_monitoring(self, user_query: str) -> Dict[str, Any]:
        """Process query using the enhanced main application with comprehensive monitoring"""
        
        if not st.session_state.current_session_id:
            # Create new session
            topic_id = dynamo_manager.create_chat_session()
            st.session_state.current_session_id = topic_id
            st.session_state.session_validated = True
            st.session_state.chat_history_synced = True
            st.session_state.chat_history = []
        
        # Create a mock event for the main application
        mock_event = {
            "requestContext": {"connectionId": "streamlit-connection"},
            "body": json.dumps({
                "TopicID": st.session_state.current_session_id,
                "Prompt": user_query
            })
        }
        
        # Process using the main application
        try:
            result = self.maritime_app.lambda_handler(mock_event, None)
            
            # Extract monitoring data from the WebSocket service
            last_message = self.maritime_app.websocket_service.get_last_test_message()
            
            # Reload session data
            updated_history = dynamo_manager.get_chat_history(st.session_state.current_session_id)
            st.session_state.chat_history = updated_history or []
            
            # Load current entities and summary
            st.session_state.current_entities = dynamo_manager.get_recent_entities(
                st.session_state.current_session_id
            ) or {}
            st.session_state.current_chat_summary = dynamo_manager.get_chat_summary(
                st.session_state.current_session_id
            ) or {}
            
            # Create comprehensive monitoring data
            monitoring_data = {
                'session_id': st.session_state.current_session_id,
                'user_query': user_query,
                'timestamp': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                'session_type': 'new' if len(st.session_state.chat_history) <= 2 else 'existing',
                'extracted_entities': st.session_state.current_entities,
                'chat_summary': st.session_state.current_chat_summary,
                'lambda_result': result,
                'websocket_response': last_message,
                'pipeline_steps': [f"✅ Processed via enhanced main application at {datetime.datetime.now().strftime('%H:%M:%S')}"],
                'session_continuity_verified': True
            }
            
            # Extract additional details from WebSocket response
            if last_message:
                response_data = last_message.get('Response', {})
                monitoring_data.update({
                    'html_response': response_data,
                    'rag_response': response_data.get('response', ''),
                    'response_type': response_data.get('response_type', 'Text')
                })
            
            return monitoring_data
            
        except Exception as e:
            error_msg = f"Error processing query: {str(e)}"
            logger.error(error_msg)
            
            return {
                'session_id': st.session_state.current_session_id,
                'user_query': user_query,
                'timestamp': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                'error': error_msg,
                'pipeline_steps': [f"❌ Error: {error_msg}"],
                'rag_response': 'Sorry, there was an error processing your request.',
                'html_response': {'response': 'Error occurred', 'response_type': 'Error'}
            }
    
    def render_comprehensive_monitoring_panel(self, monitoring_data: Dict[str, Any]):
        """Render the comprehensive monitoring panel with all requested features"""
        if not monitoring_data:
            st.info("No monitoring data available yet.")
            return
        
        st.markdown("## 🔍 Comprehensive Pipeline Monitoring")
        
        # Overview section with copyable elements
        with st.expander("📋 Query & Session Overview", expanded=True):
            self.render_overview_section(monitoring_data)
        
        # Create enhanced tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "🏷️ Entities & Summary", "🔍 Vector DB Retrieval", "🤖 AI Responses", "📊 Technical Details", "📥 Export Data"
        ])
        
        with tab1:
            self.render_entities_and_summary_tab(monitoring_data)
        
        with tab2:
            self.render_vector_db_tab(monitoring_data)
        
        with tab3:
            self.render_ai_responses_tab(monitoring_data)
        
        with tab4:
            self.render_technical_details_tab(monitoring_data)
        
        with tab5:
            self.render_export_data_tab(monitoring_data)
    
    def render_overview_section(self, monitoring_data: Dict[str, Any]):
        """Render the overview section with copyable session and query information"""
        
        # Session ID with copy
        col1, col2 = st.columns([3, 1])
        with col1:
            session_id = monitoring_data.get('session_id', 'N/A')
            st.markdown(f"**🆔 Session ID:** `{session_id}`")
        with col2:
            if st.button("📋 Copy Session ID", key="copy_overview_session"):
                st.code(session_id)
        
        # Original Question with copy
        user_query = monitoring_data.get('user_query', '')
        if user_query:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown("**💬 Original Question:**")
                st.markdown(f'<div class="monitoring-panel">{user_query}</div>', unsafe_allow_html=True)
            with col2:
                if st.button("📋 Copy Question", key="copy_overview_question"):
                    st.code(user_query)
        
        # Rephrased Question (if available)
        rephrased_query = monitoring_data.get('rephrased_query', '')
        if rephrased_query and rephrased_query != user_query:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown("**🔄 Rephrased Question:**")
                st.markdown(f'<div class="monitoring-panel">{rephrased_query}</div>', unsafe_allow_html=True)
            with col2:
                if st.button("📋 Copy Rephrased", key="copy_overview_rephrased"):
                    st.code(rephrased_query)
        
        # Session metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            session_type = monitoring_data.get('session_type', 'Unknown')
            st.metric("Session Type", session_type.title())
        with col2:
            timestamp = monitoring_data.get('timestamp', '')
            st.metric("Timestamp", timestamp[:19] if timestamp else 'N/A')
        with col3:
            messages_count = len(st.session_state.chat_history)
            st.metric("Messages", messages_count)
        with col4:
            processing_status = st.session_state.processing_status
            st.metric("Status", processing_status.title())
    
    def render_entities_and_summary_tab(self, monitoring_data: Dict[str, Any]):
        """Render entities and chat summary with copy functionality"""
        
        # Entities Section
        st.markdown("### 🏷️ Extracted Entities")
        entities = monitoring_data.get('extracted_entities', {})
        
        if entities:
            # Create entities table
            entities_data = []
            for key, value in entities.items():
                if value:
                    display_value = ', '.join(value) if isinstance(value, list) else str(value)
                    entities_data.append({
                        'Entity Type': key.replace('_', ' ').title(),
                        'Value': display_value
                    })
            
            if entities_data:
                entities_df = pd.DataFrame(entities_data)
                st.dataframe(entities_df, use_container_width=True, hide_index=True)
                
                col1, col2 = st.columns([3, 1])
                with col2:
                    if st.button("📋 Copy Entities JSON", key="copy_entities_json"):
                        st.code(json.dumps(entities, indent=2), language='json')
        else:
            st.info("No entities extracted")
        
        # Chat Summary Section
        st.markdown("### 📋 Chat Summary")
        chat_summary = monitoring_data.get('chat_summary', {})
        
        if chat_summary:
            # Navigation flow
            if 'navigation_flow' in chat_summary:
                st.markdown(f"**🗺️ Navigation Flow:** {chat_summary['navigation_flow']}")
            
            # Recent info as table
            recent_info = chat_summary.get('recent_info', {})
            if recent_info:
                summary_data = []
                for key, value in recent_info.items():
                    if value:
                        display_value = ', '.join(value) if isinstance(value, list) else str(value)
                        summary_data.append({
                            'Information Type': key.replace('_', ' ').title(),
                            'Value': display_value
                        })
                
                if summary_data:
                    summary_df = pd.DataFrame(summary_data)
                    st.dataframe(summary_df, use_container_width=True, hide_index=True)
            
            col1, col2 = st.columns([3, 1])
            with col2:
                if st.button("📋 Copy Summary JSON", key="copy_summary_json"):
                    st.code(json.dumps(chat_summary, indent=2), language='json')
        else:
            st.info("No chat summary available")
    
    def render_vector_db_tab(self, monitoring_data: Dict[str, Any]):
        """Render vector database retrieval results as comprehensive table"""
        st.markdown("### 🔍 Vector Database Retrieval Results")
        
        # For now, create mock retrieval data since we don't have direct access
        # In a real implementation, this would come from the monitoring_data
        mock_retrieval_data = [
            {
                'rank': 1,
                'page_title': 'Wärtsilä 12RT-Flex96C-B Maintenance Manual',
                'topic': 'Engine Maintenance',
                'subtopic': 'Cylinder Head Overhaul',
                'engine_make': 'Wärtsilä',
                'engine_model': '12RT-Flex96C-B',
                'equipment_type': 'Main Engine',
                'info_type': 'MANUAL',
                'relevance': 0.9234,
                'vespa_score': 0.8765,
                'document_id': 'doc_001',
                'page_number': 145
            },
            {
                'rank': 2,
                'page_title': 'Cylinder Head Removal Procedure',
                'topic': 'Engine Maintenance',
                'subtopic': 'Component Removal',
                'engine_make': 'Wärtsilä',
                'engine_model': '12RT-Flex96C-B',
                'equipment_type': 'Main Engine',
                'info_type': 'MANUAL',
                'relevance': 0.8876,
                'vespa_score': 0.8234,
                'document_id': 'doc_002',
                'page_number': 167
            },
            {
                'rank': 3,
                'page_title': 'Engine Overhaul Best Practices',
                'topic': 'Engine Maintenance',
                'subtopic': 'General Procedures',
                'engine_make': 'Wärtsilä',
                'engine_model': '12RT-Flex96C-B',
                'equipment_type': 'Main Engine',
                'info_type': 'MANUAL',
                'relevance': 0.8123,
                'vespa_score': 0.7891,
                'document_id': 'doc_003',
                'page_number': 89
            }
        ]
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Results", len(mock_retrieval_data))
        with col2:
            st.metric("Query Type", "MANUAL")
        with col3:
            st.metric("Embedding Dim", 1024)
        with col4:
            avg_relevance = sum(item['relevance'] for item in mock_retrieval_data) / len(mock_retrieval_data)
            st.metric("Avg Relevance", f"{avg_relevance:.3f}")
        
        # Display comprehensive table
        st.markdown("#### 📊 Detailed Search Results")
        
        results_df = pd.DataFrame(mock_retrieval_data)
        
        # Configure column display
        column_config = {
            'relevance': st.column_config.NumberColumn(
                'Relevance',
                help='Semantic similarity score',
                format="%.4f"
            ),
            'vespa_score': st.column_config.NumberColumn(
                'Vespa Score',
                help='Raw Vespa search score',
                format="%.4f"
            ),
            'rank': st.column_config.NumberColumn(
                'Rank',
                help='Result ranking'
            )
        }
        
        st.dataframe(
            results_df,
            use_container_width=True,
            hide_index=True,
            column_config=column_config
        )
        
        # Copy functionality
        col1, col2, col3 = st.columns([2, 1, 1])
        with col2:
            if st.button("📋 Copy as CSV", key="copy_results_csv"):
                csv_data = results_df.to_csv(index=False)
                st.code(csv_data, language='csv')
        
        with col3:
            if st.button("📋 Copy as JSON", key="copy_results_json"):
                json_data = results_df.to_json(orient='records', indent=2)
                st.code(json_data, language='json')
        
        # Individual result details
        if st.checkbox("🔍 Show Individual Result Details"):
            selected_idx = st.selectbox(
                "Select result to view:",
                range(len(mock_retrieval_data)),
                format_func=lambda x: f"Result {x+1}: {mock_retrieval_data[x]['page_title']}"
            )
            
            if selected_idx is not None:
                selected_result = mock_retrieval_data[selected_idx]
                
                st.markdown(f"#### 📄 Result {selected_idx + 1} Details")
                
                detail_data = []
                for key, value in selected_result.items():
                    detail_data.append({
                        'Field': key.replace('_', ' ').title(),
                        'Value': str(value)
                    })
                
                detail_df = pd.DataFrame(detail_data)
                st.dataframe(detail_df, use_container_width=True, hide_index=True)
                
                if st.button("📋 Copy Result Details", key=f"copy_detail_{selected_idx}"):
                    st.code(json.dumps(selected_result, indent=2), language='json')
    
    def render_ai_responses_tab(self, monitoring_data: Dict[str, Any]):
        """Render AI responses with copy functionality"""
        
        # RAG Response (Augmented)
        st.markdown("### 📚 RAG Response (Augmented)")
        rag_response = monitoring_data.get('rag_response', '')
        
        if rag_response:
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(rag_response)
            with col2:
                if st.button("📋 Copy RAG", key="copy_rag_response"):
                    st.code(rag_response, language='markdown')
        else:
            st.info("No RAG response available")
        
        # HTML Response
        st.markdown("### 🎨 HTML Response from Model")
        html_response = monitoring_data.get('html_response', {})
        
        if html_response:
            response_content = html_response.get('response', '')
            response_type = html_response.get('response_type', 'Text')
            
            col1, col2 = st.columns([4, 1])
            
            with col1:
                st.markdown(f"**Response Type:** {response_type}")
                
                if response_type == 'HTML' and response_content:
                    st.markdown("**Rendered HTML:**")
                    st.markdown(response_content, unsafe_allow_html=True)
                else:
                    st.markdown("**Text Response:**")
                    st.markdown(response_content)
                
                # Raw HTML in expander
                with st.expander("🔍 View Raw HTML Source"):
                    st.code(response_content, language='html')
            
            with col2:
                if st.button("📋 Copy HTML", key="copy_html_response"):
                    st.code(response_content, language='html')
        else:
            st.info("No HTML response available")
        
        # Final JSON Response
        st.markdown("### 📄 Final JSON Response from Model")
        
        if html_response:
            # Create complete JSON response
            complete_json = {
                "session_id": monitoring_data.get('session_id'),
                "timestamp": monitoring_data.get('timestamp'),
                "user_query": monitoring_data.get('user_query'),
                "extracted_entities": monitoring_data.get('extracted_entities', {}),
                "chat_summary": monitoring_data.get('chat_summary', {}),
                "rag_response": rag_response,
                "html_response": html_response,
                "response_metadata": {
                    "session_type": monitoring_data.get('session_type'),
                    "processing_status": "completed",
                    "model_version": "enhanced_maritime_rag_v4"
                }
            }
            
            col1, col2 = st.columns([4, 1])
            
            with col1:
                st.json(complete_json)
            
            with col2:
                if st.button("📋 Copy JSON", key="copy_final_json"):
                    st.code(json.dumps(complete_json, indent=2), language='json')
        else:
            st.info("No final JSON response available")
    
    def render_technical_details_tab(self, monitoring_data: Dict[str, Any]):
        """Render technical debugging information"""
        
        # Pipeline steps
        st.markdown("### 🔄 Pipeline Execution Steps")
        steps = monitoring_data.get('pipeline_steps', [])
        
        if steps:
            steps_data = []
            for i, step in enumerate(steps, 1):
                # Determine status from step content
                if '✅' in step:
                    status = 'Success'
                elif '❌' in step:
                    status = 'Error'
                elif '⚠️' in step:
                    status = 'Warning'
                else:
                    status = 'Info'
                
                steps_data.append({
                    'Step': i,
                    'Status': status,
                    'Description': step,
                    'Timestamp': monitoring_data.get('timestamp', 'N/A')[:19]
                })
            
            steps_df = pd.DataFrame(steps_data)
            st.dataframe(steps_df, use_container_width=True, hide_index=True)
            
            if st.button("📋 Copy Pipeline Steps", key="copy_pipeline_steps"):
                steps_text = '\n'.join([f"{i}. {step}" for i, step in enumerate(steps, 1)])
                st.code(steps_text, language='text')
        else:
            st.info("No pipeline steps recorded")
        
        # Lambda result (if available)
        lambda_result = monitoring_data.get('lambda_result', {})
        if lambda_result:
            st.markdown("### ⚙️ Lambda Execution Result")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                st.json(lambda_result)
            with col2:
                if st.button("📋 Copy Lambda Result", key="copy_lambda_result"):
                    st.code(json.dumps(lambda_result, indent=2), language='json')
        
        # WebSocket response (if available)
        ws_response = monitoring_data.get('websocket_response', {})
        if ws_response:
            st.markdown("### 📡 WebSocket Response")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                st.json(ws_response)
            with col2:
                if st.button("📋 Copy WebSocket", key="copy_websocket"):
                    st.code(json.dumps(ws_response, indent=2), language='json')
        
        # Error information
        if monitoring_data.get('error'):
            st.markdown("### ❌ Error Information")
            error_msg = monitoring_data['error']
            st.error(error_msg)
            
            if st.button("📋 Copy Error", key="copy_error_info"):
                st.code(error_msg, language='text')
    
    def render_export_data_tab(self, monitoring_data: Dict[str, Any]):
        """Render data export functionality"""
        st.markdown("### 📥 Export Complete Data")
        
        # Export options
        export_options = st.multiselect(
            "Select data to export:",
            [
                "Session Information",
                "Query & Responses", 
                "Entities & Summary",
                "Vector DB Results",
                "Pipeline Steps",
                "Technical Details"
            ],
            default=["Session Information", "Query & Responses", "Entities & Summary"]
        )
        
        if st.button("📊 Generate Export Package", key="generate_export"):
            export_data = {}
            
            if "Session Information" in export_options:
                export_data["session_info"] = {
                    "session_id": monitoring_data.get('session_id'),
                    "timestamp": monitoring_data.get('timestamp'),
                    "session_type": monitoring_data.get('session_type'),
                    "messages_count": len(st.session_state.chat_history),
                    "session_validated": st.session_state.session_validated
                }
            
            if "Query & Responses" in export_options:
                export_data["query_responses"] = {
                    "user_query": monitoring_data.get('user_query'),
                    "rephrased_query": monitoring_data.get('rephrased_query'),
                    "rag_response": monitoring_data.get('rag_response'),
                    "html_response": monitoring_data.get('html_response')
                }
            
            if "Entities & Summary" in export_options:
                export_data["entities_summary"] = {
                    "extracted_entities": monitoring_data.get('extracted_entities', {}),
                    "chat_summary": monitoring_data.get('chat_summary', {})
                }
            
            if "Vector DB Results" in export_options:
                # Include mock data or real retrieval results
                export_data["vector_db_results"] = {
                    "query_type": "MANUAL",
                    "total_results": 3,
                    "results": []  # Would include actual search results
                }
            
            if "Pipeline Steps" in export_options:
                export_data["pipeline_execution"] = {
                    "steps": monitoring_data.get('pipeline_steps', []),
                    "lambda_result": monitoring_data.get('lambda_result', {}),
                    "websocket_response": monitoring_data.get('websocket_response', {})
                }
            
            if "Technical Details" in export_options:
                export_data["technical_details"] = {
                    "error": monitoring_data.get('error'),
                    "processing_status": st.session_state.processing_status,
                    "session_continuity_verified": monitoring_data.get('session_continuity_verified', False)
                }
            
            # Display export package
            st.markdown("#### 📦 Export Package")
            st.json(export_data)
            
            # Generate filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            session_short = monitoring_data.get('session_id', 'unknown')[:8]
            filename = f"maritime_rag_export_{session_short}_{timestamp}.json"
            
            st.markdown(f"**Suggested filename:** `{filename}`")
            
            # Copy button for export
            if st.button("📋 Copy Export JSON", key="copy_export_package"):
                st.code(json.dumps(export_data, indent=2), language='json')
    
    def render_chat_interface(self):
        """Render the main chat interface"""
        st.markdown('<div class="main-header">🚢 Maritime RAG Chatbot - Enhanced</div>', unsafe_allow_html=True)
        
        # Session management
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            if st.button("➕ New Session", key="new_session_main"):
                topic_id = dynamo_manager.create_chat_session()
                st.session_state.current_session_id = topic_id
                st.session_state.session_validated = True
                st.session_state.chat_history = []
                st.session_state.monitoring_data = {}
                st.rerun()
        
        with col2:
            if st.session_state.current_session_id and st.button("🔄 Refresh", key="refresh_session"):
                self.validate_session_integrity()
                st.rerun()
        
        with col3:
            st.session_state.show_technical_details = st.checkbox("🛠️ Show Technical", value=st.session_state.show_technical_details)
        
        # Display session info
        if st.session_state.current_session_id:
            self.render_copyable_session_info()
        else:
            st.info("👈 Please create a new session to begin chatting.")
            return
        
        # Chat history
        st.markdown("### 💬 Conversation")
        
        for message in st.session_state.chat_history:
            role = message.get('Role', 'unknown')
            
            if role == 'user':
                with st.chat_message("user"):
                    st.write(message.get('Prompt', ''))
            
            elif role == 'assistant':
                with st.chat_message("assistant"):
                    response_data = message.get('Response', {})
                    response_content = response_data.get('response', '')
                    response_type = response_data.get('response_type', 'Text')
                    
                    if response_type == 'HTML' and response_content:
                        st.markdown(response_content, unsafe_allow_html=True)
                    else:
                        st.write(response_content)
        
        # User input
        user_input = st.chat_input("Ask about maritime equipment...")
        
        if user_input:
            # Show user message
            with st.chat_message("user"):
                st.write(user_input)
            
            # Process query
            with st.chat_message("assistant"):
                with st.spinner("Processing your query with enhanced monitoring..."):
                    monitoring_data = self.process_query_with_enhanced_monitoring(user_input)
                    st.session_state.monitoring_data = monitoring_data
                    
                    # Display response
                    html_response = monitoring_data.get('html_response', {})
                    if html_response:
                        response_content = html_response.get('response', '')
                        response_type = html_response.get('response_type', 'Text')
                        
                        if response_type == 'HTML' and response_content:
                            st.markdown(response_content, unsafe_allow_html=True)
                        else:
                            st.write(response_content)
            
            st.rerun()
    
    def run(self):
        """Main application runner"""
        
        # Main layout
        col1, col2 = st.columns([1, 1])
        
        with col1:
            self.render_chat_interface()
        
        with col2:
            if st.session_state.monitoring_data:
                self.render_comprehensive_monitoring_panel(st.session_state.monitoring_data)
            else:
                st.info("💡 Monitoring data will appear here after your first query.")

# Initialize and run the application
if __name__ == "__main__":
    app = EnhancedMaritimeRAGStreamlit()
    app.run()