#!/usr/bin/env python3
"""
Maritime RAG Streamlit Application Launcher
Run this script to start the Streamlit chatbot interface
"""

import sys
import os
import subprocess
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = [
        'streamlit',
        'pandas',
        'boto3',
        'python-dotenv',
        'rapidfuzz'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        logger.error(f"❌ Missing required packages: {', '.join(missing_packages)}")
        logger.info("Install them using: pip install -r streamlit_requirements.txt")
        return False
    
    return True

def check_environment():
    """Check if required environment variables are set"""
    required_env_vars = [
        'VESPA_ENDPOINT',
        'aws_access_key_id',
        'aws_secret_access_key',
        'table_name'
    ]
    
    missing_vars = []
    
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.warning(f"⚠️ Missing environment variables: {', '.join(missing_vars)}")
        logger.info("Make sure your .env file is properly configured")
        return False
    
    return True

def run_streamlit_app():
    """Launch the Streamlit application"""
    try:
        logger.info("🚀 Starting Maritime RAG Streamlit Application...")
        
        # Check dependencies
        if not check_dependencies():
            sys.exit(1)
        
        # Check environment (warn but don't exit)
        if not check_environment():
            logger.warning("⚠️ Some environment variables are missing. App may not work properly.")
        
        # Get the directory of this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        app_path = os.path.join(script_dir, "maritime_rag_streamlit_app.py")
        
        # Launch Streamlit
        cmd = [
            sys.executable, "-m", "streamlit", "run", app_path,
            "--server.port", "8501",
            "--server.address", "localhost",
            "--browser.gatherUsageStats", "false"
        ]
        
        logger.info("🌐 Starting Streamlit server on http://localhost:8501")
        logger.info("🛑 Press Ctrl+C to stop the server")
        
        subprocess.run(cmd)
        
    except KeyboardInterrupt:
        logger.info("👋 Application stopped by user")
    except Exception as e:
        logger.error(f"❌ Error starting application: {str(e)}")
        sys.exit(1)

def main():
    """Main entry point"""
    print("🚢 Maritime RAG Streamlit Application Launcher")
    print("=" * 50)
    
    # Check if running with specific arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--check":
            print("🔍 Checking dependencies and environment...")
            deps_ok = check_dependencies()
            env_ok = check_environment()
            
            if deps_ok and env_ok:
                print("✅ All checks passed!")
            else:
                print("❌ Some checks failed. See messages above.")
            return
        
        elif sys.argv[1] == "--help":
            print("""
Usage: python run_streamlit_app.py [OPTIONS]

Options:
  --check    Check dependencies and environment without starting the app
  --help     Show this help message

Examples:
  python run_streamlit_app.py          # Start the application
  python run_streamlit_app.py --check  # Check setup only
            """)
            return
    
    # Run the application
    run_streamlit_app()

if __name__ == "__main__":
    main()