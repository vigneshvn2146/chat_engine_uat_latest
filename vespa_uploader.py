#!/usr/bin/env python3
"""
Local Vespa Document Uploader - Test Script
Simplified version for local testing of document ingestion
"""

import os
import json
import hashlib
import logging
import time
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any, Tuple

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from vespa.application import Vespa

# Load environment variables from .env file
load_dotenv()

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    def __init__(self, env='uat'):
        self.env = env.lower()
        
        # Environment-specific settings
        if self.env == 'uat':
            self.vespa_endpoint = os.getenv("UAT_VESPA_ENDPOINT")
            self.cert_path = "./utils/uat_data-plane-public-cert.pem"
            self.key_path = "./utils/uat_data-plane-private-key.pem"
            self.aws_region = os.getenv("uat_aws_region_name", "ap-south-1")
            self.aws_access_key = os.getenv("uat_aws_access_key_id")
            self.aws_secret_key = os.getenv("uat_aws_secret_access_key")
        elif self.env == 'stage':
            self.vespa_endpoint = os.getenv("STAGE_VESPA_ENDPOINT")
            self.cert_path = "./utils/stage_data-plane-public-cert.pem"
            self.key_path = "./utils/stage_data-plane-private-key.pem"
            self.aws_region = os.getenv("stage_aws_region_name", "ap-south-1")
            self.aws_access_key = os.getenv("stage_aws_access_key_id")
            self.aws_secret_key = os.getenv("stage_aws_secret_access_key")
        elif self.env == 'prod':
            self.vespa_endpoint = os.getenv("PROD_VESPA_ENDPOINT")
            self.cert_path = "./utils/prod_data-plane-public-cert.pem"
            self.key_path = "./utils/prod_data-plane-private-key.pem"
            self.aws_region = os.getenv("prod_aws_region_name", "ap-south-1")
            self.aws_access_key = os.getenv("prod_aws_access_key_id")
            self.aws_secret_key = os.getenv("prod_aws_secret_access_key")
        
        self.validate()
    
    def validate(self):
        """Validate required configuration"""
        missing = []
        
        if not self.vespa_endpoint:
            missing.append(f"{self.env.upper()}_VESPA_ENDPOINT")
        if not self.aws_access_key:
            missing.append(f"{self.env}_aws_access_key_id")
        if not self.aws_secret_key:
            missing.append(f"{self.env}_aws_secret_access_key")
        if not os.path.exists(self.cert_path):
            missing.append(f"Certificate file: {self.cert_path}")
        if not os.path.exists(self.key_path):
            missing.append(f"Key file: {self.key_path}")
            
        if missing:
            print(f"\n❌ Missing configuration for {self.env} environment:")
            for item in missing:
                print(f"   - {item}")
            print(f"\n💡 Current environment variables:")
            print(f"   - UAT_VESPA_ENDPOINT: {'✓' if os.getenv('UAT_VESPA_ENDPOINT') else '❌'}")
            print(f"   - uat_aws_access_key_id: {'✓' if os.getenv('uat_aws_access_key_id') else '❌'}")
            print(f"   - uat_aws_secret_access_key: {'✓' if os.getenv('uat_aws_secret_access_key') else '❌'}")
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")

# ============================================================================
# UTILITIES
# ============================================================================

def create_env_file_template():
    """Create a template .env file"""
    template = """# Vespa Document Uploader Configuration
# Copy this template and fill in your actual values

# UAT Environment
UAT_VESPA_ENDPOINT=https://your-uat-vespa-endpoint.com
uat_aws_access_key_id=your_uat_access_key
uat_aws_secret_access_key=your_uat_secret_key
uat_aws_region_name=ap-south-1

# Stage Environment  
STAGE_VESPA_ENDPOINT=https://your-stage-vespa-endpoint.com
stage_aws_access_key_id=your_stage_access_key
stage_aws_secret_access_key=your_stage_secret_key
stage_aws_region_name=ap-south-1

# Production Environment
PROD_VESPA_ENDPOINT=https://your-prod-vespa-endpoint.com
prod_aws_access_key_id=your_prod_access_key
prod_aws_secret_access_key=your_prod_secret_key
prod_aws_region_name=ap-south-1
"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(template)
        print("📝 Created .env template file. Please fill in your actual values.")
    else:
        print("📁 .env file already exists")

def check_setup(env='uat'):
    """Check if everything is set up correctly"""
    print(f"\n🔍 Checking setup for {env} environment...")
    
    # Check .env file
    if not os.path.exists('.env'):
        print("❌ .env file not found")
        create_env_file_template()
        return False
    
    # Check utils directory and certificates
    utils_dir = './utils'
    if not os.path.exists(utils_dir):
        print(f"📁 Creating utils directory: {utils_dir}")
        os.makedirs(utils_dir, exist_ok=True)
    
    cert_files = {
        'uat': ['uat_data-plane-public-cert.pem', 'uat_data-plane-private-key.pem'],
        'stage': ['stage_data-plane-public-cert.pem', 'stage_data-plane-private-key.pem'], 
        'prod': ['prod_data-plane-public-cert.pem', 'prod_data-plane-private-key.pem']
    }
    
    missing_certs = []
    for cert_file in cert_files.get(env, []):
        cert_path = os.path.join(utils_dir, cert_file)
        if not os.path.exists(cert_path):
            missing_certs.append(cert_path)
    
    if missing_certs:
        print(f"❌ Missing certificate files:")
        for cert in missing_certs:
            print(f"   - {cert}")
        print("💡 Please add your Vespa certificate files to the utils/ directory")
        return False
    
    print("✅ Setup looks good!")
    return True

def sanitize_text(text: Optional[str]) -> str:
    """Clean up text content"""
    if not text:
        return ""
    
    # Remove problematic characters
    problematic_chars = ['\x18', '\ue02d', '\u0000', '\u001F', '\u000B', '\u000C']
    sanitized = str(text)
    for char in problematic_chars:
        sanitized = sanitized.replace(char, '')
    
    return sanitized.strip()

def generate_doc_id(content: str) -> str:
    """Generate unique document ID"""
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

def is_folder(path: str) -> bool:
    """Check if path is a folder (ends with / or no file extension)"""
    return path.endswith('/') or ('.' not in path.split('/')[-1])

def validate_document_schema(doc: dict) -> Tuple[bool, list]:
    """Validate document against Vespa schema"""
    schema_fields = {
        'id', 'vessel', 'source_url', 'person', 'document_type', 
        'document_name', 'engine_make', 'engine_model', 'info_type',
        'page_title', 'topic', 'type', 'equipment', 'embedding', 
        'url', 'text', 'page_number'
    }
    
    required_fields = {'id', 'type'}  # Minimum required fields
    errors = []
    warnings = []
    
    # Check for unknown fields (this should not happen after proper preparation)
    unknown_fields = set(doc.keys()) - schema_fields
    if unknown_fields:
        errors.append(f"Unknown fields: {unknown_fields}")
    
    # Check for missing required fields
    missing_required = required_fields - set(doc.keys())
    if missing_required:
        errors.append(f"Missing required fields: {missing_required}")
    
    # Type validations
    if 'url' in doc and not isinstance(doc['url'], list):
        errors.append("Field 'url' must be an array")
    
    if 'embedding' in doc:
        if not isinstance(doc['embedding'], dict):
            errors.append("Field 'embedding' must be a dict with 'type' and 'values'")
        elif 'values' in doc['embedding']:
            if not isinstance(doc['embedding']['values'], list):
                errors.append("Embedding 'values' must be a list")
            elif len(doc['embedding']['values']) != 1024:
                errors.append(f"Embedding must have 1024 dimensions, got {len(doc['embedding']['values'])}")
    
    # Check for empty important fields (warnings only)
    important_fields = ['text', 'page_title', 'document_name']
    empty_important = [field for field in important_fields if not doc.get(field)]
    if empty_important:
        warnings.append(f"Empty important fields: {empty_important}")
    
    return len(errors) == 0, errors + [f"WARNING: {w}" for w in warnings]

def debug_document_fields(doc_data: dict, file_key: str) -> dict:
    """Debug function to show original document fields vs schema fields"""
    schema_fields = {
        'id', 'vessel', 'source_url', 'person', 'document_type', 
        'document_name', 'engine_make', 'engine_model', 'info_type',
        'page_title', 'topic', 'type', 'equipment', 'embedding', 
        'url', 'text', 'page_number'
    }
    
    # Field mapping
    field_mapping = {
        'page_num': 'page_number',
        'page_no': 'page_number', 
        'doc_type': 'document_type',
        'doc_name': 'document_name',
        'title': 'page_title',
        'content': 'text',
        'body': 'text'
    }
    
    original_fields = set(doc_data.keys())
    unknown_fields = original_fields - schema_fields
    mappable_fields = {k: field_mapping[k] for k in unknown_fields if k in field_mapping}
    truly_unknown = unknown_fields - set(field_mapping.keys())
    
    result = {
        'file': file_key,
        'original_fields': list(original_fields),
        'schema_fields': list(schema_fields),
        'unknown_fields': list(unknown_fields),
        'mappable_fields': mappable_fields,
        'truly_unknown_fields': list(truly_unknown),
        'sample_values': {k: str(v)[:50] + "..." if len(str(v)) > 50 else str(v) 
                         for k, v in doc_data.items() if k in list(unknown_fields)[:5]}
    }
    
    return result

# ============================================================================
# S3 OPERATIONS
# ============================================================================

class S3Handler:
    def __init__(self, config: Config):
        self.s3 = boto3.client(
            's3',
            region_name=config.aws_region,
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_secret_key
        )
    
    def list_json_files(self, bucket: str, prefix: str) -> List[str]:
        """List all JSON files in S3 folder"""
        # Clean up prefix - remove leading slash and ensure trailing slash
        prefix = prefix.strip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        logger.info(f"🔍 Searching for JSON files with prefix: '{prefix}' in bucket: {bucket}")
        
        files = []
        all_files = []  # For debugging
        paginator = self.s3.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        all_files.append(key)
                        if key.endswith('.json') and not key.endswith('/'):
                            files.append(key)
            
            logger.info(f"📁 Found {len(all_files)} total files, {len(files)} JSON files")
            
            if len(all_files) > 0 and len(files) == 0:
                logger.info("📋 Sample files found (first 10):")
                for i, file in enumerate(all_files[:10]):
                    logger.info(f"   - {file}")
                if len(all_files) > 10:
                    logger.info(f"   ... and {len(all_files) - 10} more files")
            
        except Exception as e:
            logger.error(f"❌ Error listing S3 objects: {e}")
            raise
        
        return files
    
    def get_file_content(self, bucket: str, key: str) -> dict:
        """Get JSON file content from S3"""
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except ClientError as e:
            logger.error(f"Failed to get {key}: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {key}: {e}")
            raise
    
    def get_file_tags(self, bucket: str, key: str) -> dict:
        """Get S3 object tags"""
        try:
            response = self.s3.get_object_tagging(Bucket=bucket, Key=key)
            return {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        except Exception as e:
            logger.warning(f"Could not get tags for {key}: {e}")
            return {}
    
    def explore_folder(self, bucket: str, prefix: str, max_files: int = 20) -> dict:
        """Explore what's actually in an S3 folder (for debugging)"""
        prefix = prefix.strip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        logger.info(f"🔍 Exploring folder: {bucket}/{prefix}")
        
        files = []
        folders = []
        paginator = self.s3.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
                # Files in current folder
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if not obj['Key'].endswith('/'):
                            files.append(obj['Key'])
                
                # Subfolders
                if 'CommonPrefixes' in page:
                    for prefix_info in page['CommonPrefixes']:
                        folders.append(prefix_info['Prefix'])
                
                # Limit results for debugging
                if len(files) >= max_files:
                    break
            
            return {
                'prefix': prefix,
                'files': files[:max_files],
                'folders': folders,
                'total_files': len(files),
                'has_json': any(f.endswith('.json') for f in files)
            }
            
        except Exception as e:
            logger.error(f"❌ Error exploring folder: {e}")
            return {'error': str(e)}

# ============================================================================
# EMBEDDING GENERATION
# ============================================================================

class EmbeddingGenerator:
    def __init__(self, config: Config):
        self.bedrock = boto3.client(
            'bedrock-runtime',
            region_name=config.aws_region,
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_secret_key
        )
    
    def generate(self, text: str) -> List[float]:
        """Generate embedding for text"""
        if not text or not text.strip():
            return [0.0] * 1024
        
        # Truncate if too long
        text = text[:8000]
        
        try:
            body = json.dumps({"inputText": text})
            response = self.bedrock.invoke_model(
                body=body,
                modelId='amazon.titan-embed-image-v1',
                accept='application/json',
                contentType='application/json'
            )
            
            result = json.loads(response['body'].read())
            embedding = result.get('embedding')
            
            if not embedding or len(embedding) != 1024:
                logger.warning("Invalid embedding, using zero vector")
                return [0.0] * 1024
            
            return embedding
            
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            return [0.0] * 1024

# ============================================================================
# DOCUMENT PROCESSING
# ============================================================================

class DocumentProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.embedding_gen = EmbeddingGenerator(config)
    
    def prepare_document(self, doc_data: dict, source_file: str, tags: dict) -> dict:
        """Prepare document for Vespa ingestion"""
        
        # Field mapping for incoming data
        field_mapping = {
            'page_num': 'page_number',
            'page_no': 'page_number', 
            'doc_type': 'document_type',
            'doc_name': 'document_name',
            'title': 'page_title',
            'content': 'text',
            'body': 'text'
        }
        
        # Apply field mapping to doc_data
        mapped_data = {}
        for key, value in doc_data.items():
            mapped_key = field_mapping.get(key, key)
            mapped_data[mapped_key] = value
        
        # Required fields with defaults - only include schema fields
        doc = {
            "id": mapped_data.get('id') or generate_doc_id(mapped_data.get('text', '')),
            "vessel": sanitize_text(mapped_data.get('vessel', '')),
            "source_url": sanitize_text(mapped_data.get('source_url', '')),
            "person": sanitize_text(mapped_data.get('person', '')),
            "document_type": sanitize_text(mapped_data.get('document_type', '')),
            "document_name": sanitize_text(mapped_data.get('document_name', '')),
            "engine_make": sanitize_text(mapped_data.get('engine_make', '')),
            "engine_model": sanitize_text(mapped_data.get('engine_model', '')),
            "page_title": sanitize_text(mapped_data.get('page_title', '')),
            "topic": sanitize_text(mapped_data.get('topic', '')),
            "type": "text",
            "equipment": sanitize_text(mapped_data.get('equipment', '')),
            "url": mapped_data.get('url', []) if isinstance(mapped_data.get('url'), list) else [mapped_data.get('url', '')],
            "text": sanitize_text(mapped_data.get('text', '')),
            "page_number": str(mapped_data.get('page_number', '0')),
            "info_type": mapped_data.get('info_type', '')
        }
        
        # Add tags only if they are schema fields
        schema_fields = {
            'id', 'vessel', 'source_url', 'person', 'document_type', 
            'document_name', 'engine_make', 'engine_model', 'info_type',
            'page_title', 'topic', 'type', 'equipment', 'embedding', 
            'url', 'text', 'page_number'
        }
        
        for key, value in tags.items():
            if key in schema_fields:
                doc[key] = value
        
        # Generate embedding
        text_for_embedding = f"""document_name: {doc['document_name']}
topic: {doc['topic']}
title: {doc['page_title']}
text: {doc['text']}"""
        
        embedding = self.embedding_gen.generate(text_for_embedding)
        doc['embedding'] = {
            "type": "tensor<float>(x[1024])",
            "values": embedding
        }
        
        return doc

# ============================================================================
# VESPA OPERATIONS
# ============================================================================

class VespaClient:
    def __init__(self, config: Config):
        self.config = config
        self.app = None
    
    def connect(self):
        """Connect to Vespa"""
        try:
            self.app = Vespa(
                url=self.config.vespa_endpoint,
                cert=self.config.cert_path,
                key=self.config.key_path
            )
            logger.info("Connected to Vespa successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Vespa: {e}")
            raise
    
    def feed_document(self, document: dict) -> bool:
        """Feed single document to Vespa"""
        try:
            # Ensure document only contains schema fields
            schema_fields = {
                'id', 'vessel', 'source_url', 'person', 'document_type', 
                'document_name', 'engine_make', 'engine_model', 'info_type',
                'page_title', 'topic', 'type', 'equipment', 'embedding', 
                'url', 'text', 'page_number'
            }
            
            clean_document = {k: v for k, v in document.items() if k in schema_fields}
            
            # Validate required fields
            if not clean_document.get('id'):
                logger.error("Document missing required 'id' field")
                return False
            
            # Log document info for debugging
            logger.debug(f"Feeding document with {len(clean_document)} fields: {list(clean_document.keys())}")
            
            response = self.app.feed_data_point(
                schema="doc",
                data_id=clean_document["id"],
                fields=clean_document
            )
            
            if response.status_code == 200:
                logger.info(f"✓ Fed document: {clean_document['id'][:16]}...")
                return True
            else:
                logger.error(f"✗ Failed to feed document: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"✗ Error feeding document: {e}")
            return False

# ============================================================================
# MAIN UPLOADER CLASS
# ============================================================================

class VespaUploader:
    def __init__(self, environment='uat'):
        self.config = Config(environment)
        self.s3 = S3Handler(self.config)
        self.processor = DocumentProcessor(self.config)
        self.vespa = VespaClient(self.config)
        
    def upload_single_file(self, bucket: str, file_key: str) -> dict:
        """Upload a single JSON file"""
        start_time = time.time()
        
        try:
            logger.info(f"Processing file: {bucket}/{file_key}")
            
            # Get file content and tags
            doc_data = self.s3.get_file_content(bucket, file_key)
            tags = self.s3.get_file_tags(bucket, file_key)
            
            # Prepare document
            document = self.processor.prepare_document(doc_data, file_key, tags)
            
            # Validate document before feeding
            is_valid, errors = validate_document_schema(document)
            if not is_valid:
                return {
                    'status': 'error',
                    'file': file_key,
                    'error': f"Schema validation failed: {errors}",
                    'documents_processed': 0,
                    'processing_time': time.time() - start_time
                }
            
            # Connect to Vespa and feed
            self.vespa.connect()
            success = self.vespa.feed_document(document)
            
            return {
                'status': 'success' if success else 'failed',
                'file': file_key,
                'documents_processed': 1 if success else 0,
                'processing_time': time.time() - start_time
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'file': file_key,
                'error': str(e),
                'documents_processed': 0,
                'processing_time': time.time() - start_time
            }
    
    def upload_folder(self, bucket: str, folder_path: str, max_workers: int = 3) -> dict:
        """Upload all JSON files in a folder"""
        start_time = time.time()
        
        try:
            # Clean up folder path
            folder_path = folder_path.strip('/')
            logger.info(f"📂 Processing folder: {bucket}/{folder_path}")
            
            # List all JSON files
            files = self.s3.list_json_files(bucket, folder_path)
            if not files:
                return {
                    'status': 'warning',
                    'message': 'No JSON files found',
                    'folder': folder_path,
                    'files_found': 0,
                    'processing_time': time.time() - start_time
                }
            
            logger.info(f"✅ Found {len(files)} JSON files to process")
            
            # Connect to Vespa once
            self.vespa.connect()
            
            # Process files concurrently
            results = []
            successful = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._process_file_for_folder, bucket, file_path): file_path 
                    for file_path in files
                }
                
                for future in as_completed(futures):
                    file_path = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                        if result['success']:
                            successful += 1
                        logger.info(f"{'✓' if result['success'] else '✗'} {file_path}")
                    except Exception as e:
                        logger.error(f"✗ {file_path}: {e}")
                        results.append({'file': file_path, 'success': False, 'error': str(e)})
            
            return {
                'status': 'success' if successful > 0 else 'failed',
                'folder': folder_path,
                'files_found': len(files),
                'files_processed': successful,
                'documents_processed': successful,
                'processing_time': time.time() - start_time,
                'results': results
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'folder': folder_path,
                'error': str(e),
                'processing_time': time.time() - start_time
            }
    
    def _process_file_for_folder(self, bucket: str, file_key: str) -> dict:
        """Process single file for folder upload (internal method)"""
        try:
            doc_data = self.s3.get_file_content(bucket, file_key)
            tags = self.s3.get_file_tags(bucket, file_key)
            document = self.processor.prepare_document(doc_data, file_key, tags)
            
            # Validate document before feeding
            is_valid, errors = validate_document_schema(document)
            if not is_valid:
                logger.error(f"Document validation failed for {file_key}: {errors}")
                return {
                    'file': file_key,
                    'success': False,
                    'error': f"Schema validation failed: {errors}"
                }
            
            success = self.vespa.feed_document(document)
            
            return {
                'file': file_key,
                'success': success
            }
        except Exception as e:
            return {
                'file': file_key,
                'success': False,
                'error': str(e)
            }
    
    def explore_folder(self, bucket: str, folder_path: str) -> dict:
        """Explore what's in a folder (for debugging)"""
        return self.s3.explore_folder(bucket, folder_path)

# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Upload documents to Vespa')
    parser.add_argument('bucket', help='S3 bucket name')
    parser.add_argument('path', help='File path or folder path in S3')
    parser.add_argument('--env', choices=['uat', 'stage', 'prod'], default='uat', 
                       help='Environment (default: uat)')
    parser.add_argument('--workers', type=int, default=3,
                       help='Number of concurrent workers for folder processing')
    
    args = parser.parse_args()
    
    try:
        uploader = VespaUploader(args.env)
        
        if is_folder(args.path):
            result = uploader.upload_folder(args.bucket, args.path, args.workers)
        else:
            result = uploader.upload_single_file(args.bucket, args.path)
        
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(json.dumps({
            'status': 'error',
            'error': str(e)
        }, indent=2))

if __name__ == '__main__':
    # main()
    from vespa_uploader import VespaUploader

    # Initialize uploader
    uploader = VespaUploader('uat')

    bucket = 'synergy-oe-propulsionpro'

    folders = ['uat/silver/pdf/oem/SAUER (WP 311 L) IM-22-E AIR COMPRESSOR - OPERATING MANUAL/',
            'uat/silver/pdf/oem/Sauer (SC 15-52) IM-22-C AIR COMPRESSOR -OPERATING INSTRUCTIONS/',
            'uat/silver/pdf/oem/M-202 INSTRUCTION BOOK HITACHI-MAN B&W DIESEL ENGINE BOOK 2;S.NO.121/',
            'uat/silver/pdf/oem/M-201 INSTRUCTION BOOK HITACHI-MAN B&W DIESEL ENGINE BOOK 1;S.NO.121/'
            ]
    
    for folder in folders:
        result = uploader.upload_folder(bucket, folder_path=folder)
        print(result)