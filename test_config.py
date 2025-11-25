#!/usr/bin/env python3
"""
Configuration Test Script
Tests the multi-environment configuration setup for UAT, STAGE, and PROD
"""

import os
import sys
from dotenv import load_dotenv

def test_environment_config(instance):
    """Test configuration for a specific environment"""
    print(f"\n{'='*60}")
    print(f"TESTING {instance.upper()} ENVIRONMENT")
    print(f"{'='*60}")
    
    # Set environment variable
    os.environ['INSTANCE'] = instance
    
    try:
        # Import config after setting environment variable
        from config import MaritimeConfig
        
        # Create fresh config instance
        config = MaritimeConfig()
        
        # Test basic configuration
        print(f"✅ Instance: {config.get_instance()}")
        print(f"✅ AWS Region: {config.aws_region}")
        print(f"✅ Table Name: {config.table_name}")
        
        # Test Vespa configuration
        vespa_config = config.get_vespa_config()
        print(f"✅ Vespa Endpoint: {vespa_config['endpoint']}")
        print(f"✅ Certificate Path: {vespa_config['cert_file_path']}")
        print(f"✅ Key Path: {vespa_config['key_file_path']}")
        print(f"✅ Application: {vespa_config['application']}")
        
        # Test AWS credentials
        aws_creds = config.get_aws_credentials()
        print(f"✅ AWS Access Key: {'***' + aws_creds['aws_access_key_id'][-4:] if aws_creds['aws_access_key_id'] else 'NOT SET'}")
        print(f"✅ AWS Secret Key: {'***' + aws_creds['aws_secret_access_key'][-4:] if aws_creds['aws_secret_access_key'] else 'NOT SET'}")
        
        # Test WebSocket
        print(f"✅ WebSocket: {config.ws_endpoint}")
        
        # Test model configuration
        models = config.get_model_configs()
        print(f"✅ Embedding Model: {models['titan_embed']}")
        print(f"✅ Generation Model: {models['llama']}")
        
        # Check file existence
        cert_exists = os.path.exists(vespa_config['cert_file_path'])
        key_exists = os.path.exists(vespa_config['key_file_path'])
        print(f"{'✅' if cert_exists else '❌'} Certificate file exists: {cert_exists}")
        print(f"{'✅' if key_exists else '❌'} Key file exists: {key_exists}")
        
        # Configuration summary
        summary = config.get_configuration_summary()
        print(f"\n📋 Configuration Summary:")
        for key, value in summary.items():
            if 'configured' in key:
                status = "✅" if value else "❌"
                print(f"   {status} {key}: {value}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing {instance} configuration: {str(e)}")
        return False

def test_all_environments():
    """Test all environment configurations"""
    print("🧪 MARITIME RAG CONFIGURATION TESTER")
    print("This script tests the configuration for all environments")
    
    # Load environment variables
    load_dotenv()
    
    environments = ['uat', 'stage', 'prod']
    results = {}
    
    for env in environments:
        try:
            # Clear any cached config modules
            if 'config' in sys.modules:
                del sys.modules['config']
            
            results[env] = test_environment_config(env)
        except Exception as e:
            print(f"❌ Failed to test {env}: {str(e)}")
            results[env] = False
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    
    for env, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{env.upper()}: {status}")
    
    all_passed = all(results.values())
    print(f"\nOverall: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    
    return all_passed

def test_single_environment():
    """Test configuration for the current INSTANCE environment variable"""
    instance = os.getenv('INSTANCE', 'uat').lower()
    print(f"🧪 Testing current environment: {instance.upper()}")
    
    load_dotenv()
    return test_environment_config(instance)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Maritime RAG Configuration')
    parser.add_argument('--env', choices=['uat', 'stage', 'prod'], 
                       help='Test specific environment')
    parser.add_argument('--all', action='store_true', 
                       help='Test all environments')
    parser.add_argument('--current', action='store_true', 
                       help='Test current INSTANCE environment')
    
    args = parser.parse_args()
    
    if args.all:
        success = test_all_environments()
    elif args.env:
        success = test_environment_config(args.env)
    elif args.current:
        success = test_single_environment()
    else:
        print("Usage:")
        print("  python test_config.py --all           # Test all environments")
        print("  python test_config.py --env uat       # Test specific environment")
        print("  python test_config.py --current       # Test current INSTANCE")
        sys.exit(1)
    
    sys.exit(0 if success else 1)