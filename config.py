"""
Configuration Module - Multi-Environment Support
Centralized configuration management for the maritime chat system.
Supports UAT, STAGE, and PROD environments based on INSTANCE variable.
"""

import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MaritimeConfig:
    """Centralized configuration management with multi-environment support"""
    
    def __init__(self):
        """Initialize configuration by loading environment variables"""
        load_dotenv()
        self._load_config()
    
    def _load_config(self):
        """Load all configuration from environment variables based on INSTANCE"""
        # Get the instance type from environment variable
        self.instance = os.getenv('INSTANCE', "uat").lower()
        logger.info(f"🏭 Loading configuration for instance: {self.instance.upper()}")
        
        # Load environment-specific configuration
        self._load_environment_config()
        
        # Load common configuration (models, search settings, etc.)
        self._load_common_config()
        
        # Validate critical configuration
        self._validate_config()
    
    def _load_environment_config(self):
        """Load environment-specific configuration based on INSTANCE"""
        if self.instance == 'uat':
            self.env_config = {
                "cert_path": "./utils/uat_data-plane-public-cert.pem",
                "key_path": "./utils/uat_data-plane-private-key.pem",
                "application": "uatmultimodelapp",
                "endpoint": os.getenv("UAT_VESPA_ENDPOINT"),
                "aws_region": os.getenv("uat_aws_region_name", "ap-south-1"),
                "aws_access_key": os.getenv("uat_aws_access_key_id"),
                "aws_secret_key": os.getenv("uat_aws_secret_access_key"),
                "websocket": os.getenv("uat_websocket"),
                "table_name": os.getenv("uat_table_name", "maritime_chat_table-uat"),
                "user_api_url": os.getenv("uat_user_api_url")
            }
        elif self.instance == 'stage':
            self.env_config = {
                "cert_path": "./utils/stage_data-plane-public-cert.pem",
                "key_path": "./utils/stage_data-plane-private-key.pem",
                "application": "testmultimodelapp",
                "endpoint": os.getenv("STAGE_VESPA_ENDPOINT"),
                "aws_region": os.getenv("stage_aws_region_name", "ap-south-1"),
                "aws_access_key": os.getenv("stage_aws_access_key_id"),
                "aws_secret_key": os.getenv("stage_aws_secret_access_key"),
                "websocket": os.getenv("stage_websocket"),
                "table_name": os.getenv("stage_table_name", "maritime_chat_table-stage"),
                "user_api_url": os.getenv("stage_user_api_url")
            }
        elif self.instance == 'prod':
            self.env_config = {
                "cert_path": "./utils/prod_data-plane-public-cert.pem",
                "key_path": "./utils/prod_data-plane-private-key.pem",
                "application": "prodmultimodelapp",
                "endpoint": os.getenv("PROD_VESPA_ENDPOINT"),
                "aws_region": os.getenv("prod_aws_region_name", "ap-south-1"),
                "aws_access_key": os.getenv("prod_aws_access_key_id"),
                "aws_secret_key": os.getenv("prod_aws_secret_access_key"),
                "websocket": os.getenv("prod_websocket"),
                "table_name": os.getenv("prod_table_name", "maritime_chat_table-prod"),
                "user_api_url": os.getenv("prod_user_api_url")
            }
        else:
            raise ValueError(f"Unknown instance: {self.instance}. Supported instances: uat, stage, prod")
        
        # Set instance-specific properties for backward compatibility
        self.vespa_endpoint = self.env_config["endpoint"]
        self.cert_file_path = self.env_config["cert_path"]
        self.key_file_path = self.env_config["key_path"]
        self.ws_endpoint = self.env_config["websocket"]
        self.user_api_url = self.env_config["user_api_url"]
        self.aws_access_key = self.env_config["aws_access_key"]
        self.aws_secret_key = self.env_config["aws_secret_key"]
        self.aws_region = self.env_config["aws_region"]
        self.table_name = self.env_config["table_name"]
        
        logger.info(f"✅ Environment configuration loaded for {self.instance.upper()}")
        logger.info(f"   - Vespa App: {self.env_config['application']}")
        logger.info(f"   - AWS Region: {self.aws_region}")
        logger.info(f"   - DynamoDB Table: {self.table_name}")
    
    def _load_common_config(self):
        """Load common configuration shared across all environments"""
        # Database Configuration
        self.csv_path = './utils/MAKE_MODEL_REF.xlsx'
        
        # Model Configuration - now using environment variables
        self.titan_embed_model = os.getenv("EMBEDDING_MODEL", "amazon.titan-embed-image-v1")
        self.llama_model = os.getenv("GENERATION_MODEL", "meta.llama3-8b-instruct-v1:0")
        self.mistral_model = os.getenv("GENERATION_MODEL", "mistral.mistral-large-2402-v1:0")
        
        # Search Configuration
        self.vespa_search_limit = 10
        self.max_context_results = 20
        self.search_timeout = 10
        
        # Entity Configuration
        self.entity_similarity_threshold = 85.0
        self.entity_fuzzy_algorithm = "WRatio"
        self.entity_top_results = 1
        self.entity_confidence_threshold = 0.5
        
        # Test Mode Configuration
        self.test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
        
        logger.info(f"✅ Common configuration loaded")
        logger.info(f"   - Embedding Model: {self.titan_embed_model}")
        logger.info(f"   - Generation Model: {self.llama_model}")
    
    def _validate_config(self):
        """Validate critical configuration values"""
        required_configs = [
            ('vespa_endpoint', self.vespa_endpoint),
            ('aws_access_key', self.aws_access_key),
            ('aws_secret_key', self.aws_secret_key),
            ('instance', self.instance)
        ]
        
        missing_configs = []
        for name, value in required_configs:
            if not value:
                missing_configs.append(name)
        
        if missing_configs and not self.test_mode:
            logger.warning(f"⚠️ Missing configurations for {self.instance.upper()}: {missing_configs}")
            logger.warning("Some features may not work properly")
        else:
            logger.info(f"✅ Configuration validation passed for {self.instance.upper()}")
    
    def get_instance(self):
        """Get current instance name"""
        return self.instance
    
    def get_environment_config(self):
        """Get complete environment configuration"""
        return self.env_config.copy()
    
    def get_aws_credentials(self):
        """Get AWS credentials as a dictionary"""
        return {
            'aws_access_key_id': self.aws_access_key,
            'aws_secret_access_key': self.aws_secret_key,
            'region_name': self.aws_region
        }
    
    def get_user_api_url(self):
        """Return the user API endpoint for fetching current user details"""
        if not self.user_api_url:
            logger.warning(f"⚠️ No user_api_url found for {self.instance.upper()} environment.")
        return self.user_api_url
    
    def get_vespa_config(self):
        """Get Vespa configuration"""
        return {
            'endpoint': self.vespa_endpoint,
            'cert_file_path': self.cert_file_path,
            'key_file_path': self.key_file_path,
            'application': self.env_config['application']
        }
    
    def get_model_configs(self):
        """Get model configuration"""
        return {
            'titan_embed': self.titan_embed_model,
            'llama': self.llama_model,
            'mistral': self.mistral_model
        }
    
    def get_entity_config(self):
        """Get entity extraction configuration"""
        return {
            'similarity_threshold': self.entity_similarity_threshold,
            'fuzzy_algorithm': self.entity_fuzzy_algorithm,
            'top_results': self.entity_top_results,
            'confidence_threshold': self.entity_confidence_threshold,
            'csv_path': self.csv_path
        }
    
    def is_test_mode(self):
        """Check if running in test mode"""
        return self.test_mode
    
    def get_configuration_summary(self):
        """Get a summary of current configuration for logging/debugging"""
        return {
            "instance": self.instance,
            "aws_region": self.aws_region,
            "vespa_application": self.env_config['application'],
            "table_name": self.table_name,
            "embedding_model": self.titan_embed_model,
            "generation_model": self.llama_model,
            "test_mode": self.test_mode,
            "vespa_endpoint_configured": bool(self.vespa_endpoint),
            "aws_credentials_configured": bool(self.aws_access_key and self.aws_secret_key),
            "websocket_configured": bool(self.ws_endpoint)
        }

# Global configuration instance
config = MaritimeConfig()

# Log configuration summary on module load
logger.info("📋 Configuration Summary:")
summary = config.get_configuration_summary()
for key, value in summary.items():
    logger.info(f"   {key}: {value}")