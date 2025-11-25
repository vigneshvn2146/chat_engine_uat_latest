"""
Session Management Tester - Test with Different Topic IDs
Allows explicit control over topic IDs to test session management features
"""

import json
import uuid
from app import lambda_handler

class SessionManagementTester:
    """Test session management with explicit topic ID control"""
    
    def __init__(self):
        self.sessions = {}  # Track multiple sessions
        
    def create_test_event(self, topic_id: str, query: str, connection_id: str = "test-connection-123"):
        """Create test event with specific topic ID"""
        return {
            "requestContext": {
                "connectionId": connection_id,
                "routeKey": "$default"
            },
            "body": json.dumps({
                "TopicID": topic_id,
                "Prompt": query
            })
        }
    
    def run_test_with_topic_id(self, topic_id: str, query: str, description: str = ""):
        """Run test with specific topic ID"""
        print(f"\n{'='*80}")
        if description:
            print(f"🧪 TEST: {description}")
        print(f"📝 QUERY: {query}")
        print(f"🆔 TOPIC_ID: {topic_id or 'EMPTY (New Session)'}")
        print(f"{'='*80}")
        
        try:
            # Create and run test event
            event = self.create_test_event(topic_id, query)
            result = lambda_handler(event, None)
            
            # Parse response
            status = result.get('statusCode')
            body = json.loads(result.get('body', '{}'))
            returned_topic_id = body.get('topic_id')
            message = body.get('message')
            
            # Display results
            print(f"✅ STATUS: {status}")
            print(f"📋 MESSAGE: {message}")
            print(f"🔄 RETURNED_TOPIC_ID: {returned_topic_id}")
            
            # Update session tracking
            if returned_topic_id:
                if topic_id:
                    # Existing session
                    if topic_id in self.sessions:
                        self.sessions[topic_id]['message_count'] += 1
                        self.sessions[topic_id]['last_query'] = query
                    else:
                        print(f"⚠️ WARNING: Topic ID {topic_id} not found in session tracking")
                else:
                    # New session created
                    self.sessions[returned_topic_id] = {
                        'created_with_query': query,
                        'message_count': 1,
                        'last_query': query
                    }
                    print(f"🆕 NEW SESSION CREATED: {returned_topic_id}")
            
            return result, returned_topic_id
            
        except Exception as e:
            print(f"❌ ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return None, None
    
    def test_session_isolation(self):
        """Test that different sessions are isolated"""
        print("\n🔒 TESTING SESSION ISOLATION")
        print("="*80)
        
        # Create Session A
        print("\n📋 Creating Session A...")
        result_a, session_a = self.run_test_with_topic_id(
            "", 
            "Wärtsilä 6L46 engine problems",
            "Session A - Initial Query"
        )
        
        # Create Session B
        print("\n📋 Creating Session B...")
        result_b, session_b = self.run_test_with_topic_id(
            "",
            "Caterpillar 3516 maintenance procedures", 
            "Session B - Initial Query"
        )
        
        # Test Session A continuation
        print("\n📋 Session A Continuation...")
        self.run_test_with_topic_id(
            session_a,
            "What about the fuel injection system?",
            "Session A - Follow-up (should remember Wärtsilä)"
        )
        
        # Test Session B continuation  
        print("\n📋 Session B Continuation...")
        self.run_test_with_topic_id(
            session_b,
            "What about the cooling system?",
            "Session B - Follow-up (should remember Caterpillar)"
        )
        
        print(f"\n🎯 ISOLATION RESULT:")
        print(f"Session A ID: {session_a}")
        print(f"Session B ID: {session_b}")
        print(f"Sessions are isolated: {session_a != session_b}")
    
    def test_session_continuity(self, base_topic_id: str = None):
        """Test session continuity with specific topic ID"""
        print("\n🔄 TESTING SESSION CONTINUITY")
        print("="*80)
        
        # Use provided topic ID or create new session
        if base_topic_id:
            topic_id = base_topic_id
            print(f"Using existing session: {topic_id}")
        else:
            # Create new session
            result, topic_id = self.run_test_with_topic_id(
                "",
                "Wärtsilä 6L46 engine maintenance",
                "Initial Query - Create Session"
            )
        
        if not topic_id:
            print("❌ Failed to get topic ID for continuity test")
            return
        
        # Test continuity with multiple follow-up queries
        follow_up_queries = [
            "What about the fuel injection system?",
            "How do I maintain the cylinder head?", 
            "What are common problems with this engine?",
            "Recommended maintenance intervals?"
        ]
        
        for i, query in enumerate(follow_up_queries, 1):
            print(f"\n📋 Continuity Test {i}...")
            self.run_test_with_topic_id(
                topic_id,
                query,
                f"Session Continuity {i} - Should remember previous context"
            )
        
        print(f"\n🎯 CONTINUITY RESULT:")
        print(f"Session ID used consistently: {topic_id}")
        print(f"Total queries in session: {self.sessions.get(topic_id, {}).get('message_count', 0)}")
    
    def test_new_vs_existing_session_behavior(self):
        """Test different behavior between new and existing sessions"""
        print("\n🆕🔄 TESTING NEW vs EXISTING SESSION BEHAVIOR")
        print("="*80)
        
        # Test 1: New session with insufficient entities
        print("\n📋 Test 1: New Session - Insufficient Entities...")
        result_1, session_1 = self.run_test_with_topic_id(
            "",
            "Engine overheating problems",
            "New Session - Should request entity information"
        )
        
        # Test 2: Same query in existing session (should use context)
        if session_1:
            print("\n📋 Test 2: Existing Session - Same Query...")
            self.run_test_with_topic_id(
                session_1,
                "Engine overheating problems", 
                "Existing Session - Should use any stored context"
            )
        
        # Test 3: New session with complete entities
        print("\n📋 Test 3: New Session - Complete Entities...")
        result_3, session_3 = self.run_test_with_topic_id(
            "",
            "Wärtsilä 6L46 engine overheating problems",
            "New Session - Should proceed with entities"
        )
        
        # Test 4: Follow-up in existing session
        if session_3:
            print("\n📋 Test 4: Existing Session - Follow-up...")
            self.run_test_with_topic_id(
                session_3,
                "What about preventive measures?",
                "Existing Session - Should remember Wärtsilä 6L46 context"
            )
    
    def test_with_custom_topic_ids(self):
        """Test with predefined custom topic IDs"""
        print("\n🎯 TESTING WITH CUSTOM TOPIC IDs")
        print("="*80)
        
        # Define custom topic IDs for testing
        custom_sessions = {
            "maritime-session-001": "Wärtsilä engine session",
            "maritime-session-002": "Caterpillar engine session", 
            "maritime-session-003": "Vessel maintenance session"
        }
        
        # Create sessions with custom IDs
        for custom_id, description in custom_sessions.items():
            print(f"\n📋 Testing Custom ID: {custom_id}")
            self.run_test_with_topic_id(
                custom_id,
                f"Test query for {description}",
                f"Custom Topic ID Test - {description}"
            )
    
    def test_session_context_updates(self):
        """Test how session context gets updated through conversation"""
        print("\n🔄 TESTING SESSION CONTEXT UPDATES")
        print("="*80)
        
        # Start with minimal information
        result, session_id = self.run_test_with_topic_id(
            "",
            "Engine problems",
            "Step 1 - Minimal information"
        )
        
        if not session_id:
            print("❌ Failed to create session")
            return
        
        # Gradually provide more information
        context_building_queries = [
            "It's a Wärtsilä engine",
            "Model 6L46 specifically", 
            "The vessel is MV Atlantic",
            "Fuel injection system issues",
            "What maintenance procedures are recommended?"
        ]
        
        for i, query in enumerate(context_building_queries, 2):
            print(f"\n📋 Step {i} - Context Building...")
            self.run_test_with_topic_id(
                session_id,
                query,
                f"Context Update {i} - Adding information progressively"
            )
        
        print(f"\n🎯 CONTEXT BUILDING RESULT:")
        print(f"Session ID: {session_id}")
        print("Context should have been built progressively through conversation")
    
    def display_session_summary(self):
        """Display summary of all tracked sessions"""
        print("\n📊 SESSION SUMMARY")
        print("="*60)
        
        if not self.sessions:
            print("No sessions tracked")
            return
        
        for session_id, info in self.sessions.items():
            print(f"\n🆔 Session: {session_id}")
            print(f"   Created with: {info['created_with_query']}")
            print(f"   Message count: {info['message_count']}")
            print(f"   Last query: {info['last_query']}")
    
    def run_comprehensive_session_test(self):
        """Run comprehensive session management test suite"""
        print("🚀 COMPREHENSIVE SESSION MANAGEMENT TEST SUITE")
        print("="*80)
        
        # Test 1: Session Isolation
        self.test_session_isolation()
        
        input("\n⏸️ Press Enter to continue to Session Continuity test...")
        
        # Test 2: Session Continuity
        self.test_session_continuity()
        
        input("\n⏸️ Press Enter to continue to New vs Existing behavior test...")
        
        # Test 3: New vs Existing Session Behavior
        self.test_new_vs_existing_session_behavior()
        
        input("\n⏸️ Press Enter to continue to Context Updates test...")
        
        # Test 4: Session Context Updates
        self.test_session_context_updates()
        
        # Final Summary
        self.display_session_summary()

# =============================================================================
# QUICK TEST FUNCTIONS
# =============================================================================

def test_specific_topic_id(topic_id: str, query: str):
    """Quick test with specific topic ID"""
    tester = SessionManagementTester()
    return tester.run_test_with_topic_id(topic_id, query, f"Specific Topic ID Test")

def test_multiple_sessions():
    """Quick test with multiple sessions"""
    tester = SessionManagementTester()
    
    # Session 1
    print("Creating Session 1...")
    result1, session1 = tester.run_test_with_topic_id("", "Wärtsilä problems", "Session 1")
    
    # Session 2  
    print("Creating Session 2...")
    result2, session2 = tester.run_test_with_topic_id("", "Caterpillar maintenance", "Session 2")
    
    # Continue Session 1
    print("Continuing Session 1...")
    tester.run_test_with_topic_id(session1, "Fuel injection issues", "Session 1 continuation")
    
    # Continue Session 2
    print("Continuing Session 2...")
    tester.run_test_with_topic_id(session2, "Cooling system", "Session 2 continuation")
    
    tester.display_session_summary()

def test_session_persistence():
    """Test session persistence across multiple interactions"""
    tester = SessionManagementTester()
    
    # Create a session
    result, session_id = tester.run_test_with_topic_id(
        "", 
        "Wärtsilä 6L46 problems", 
        "Create persistent session"
    )
    
    print(f"\n🆔 Created session: {session_id}")
    print("You can now use this session ID for further testing:")
    print(f"test_specific_topic_id('{session_id}', 'Your follow-up query')")
    
    return session_id

if __name__ == "__main__":
    print("🎯 SESSION MANAGEMENT TESTER")
    print("Choose testing mode:")
    print("1. Comprehensive session test suite")
    print("2. Test session isolation (multiple sessions)")
    print("3. Test session continuity (single session)")
    print("4. Test with custom topic ID")
    print("5. Test session context updates")
    print("6. Quick multiple sessions test")
    print("7. Exit")
    
    try:
        choice = input("\nEnter choice (1-7): ").strip()
        
        tester = SessionManagementTester()
        
        if choice == "1":
            tester.run_comprehensive_session_test()
        elif choice == "2":
            tester.test_session_isolation()
        elif choice == "3":
            tester.test_session_continuity()
        elif choice == "4":
            custom_id = input("Enter custom topic ID: ").strip()
            query = input("Enter query: ").strip()
            if custom_id and query:
                tester.run_test_with_topic_id(custom_id, query, "Custom Topic ID Test")
        elif choice == "5":
            tester.test_session_context_updates()
        elif choice == "6":
            test_multiple_sessions()
        elif choice == "7":
            print("👋 Goodbye!")
        else:
            print("Invalid choice. Running comprehensive test...")
            tester.run_comprehensive_session_test()
            
    except KeyboardInterrupt:
        print("\n👋 Application terminated by user")
    except Exception as e:
        print(f"❌ Application error: {str(e)}")
        import traceback
        traceback.print_exc()