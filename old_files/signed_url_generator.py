"""
Signed URL Generator Module
Handles conversion of S3 URLs to signed URLs for secure access
"""
import re
import boto3
import logging
from typing import Dict, List
from urllib.parse import urlparse
from config import config

logger = logging.getLogger(__name__)

def parse_s3_url(s3_url: str) -> tuple:
    """Parse S3 URL to extract bucket and key"""
    try:
        # s3://bucket-name/path/to/file -> (bucket-name, path/to/file)
        parsed = urlparse(s3_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        return bucket, key
    except Exception as e:
        logger.error(f"❌ Error parsing S3 URL {s3_url}: {str(e)}")
        raise

def generate_signed_url_for_s3_path(s3_url: str, expiry_hours: int = 24) -> str:
    """Convert s3://bucket/key to signed URL"""
    try:
        if not s3_url.startswith('s3://'):
            logger.warning(f"⚠️ Non-S3 URL provided: {s3_url}")
            return s3_url  # Return as-is if not S3
        
        bucket, key = parse_s3_url(s3_url)
        
        if not bucket or not key:
            logger.warning(f"⚠️ Invalid S3 URL format: {s3_url}")
            return s3_url
        
        s3_client = boto3.client('s3', **config.get_aws_credentials())
        
        signed_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiry_hours * 3600
        )
        
        logger.debug(f"✅ Generated signed URL for: {s3_url}")
        return signed_url
        
    except Exception as e:
        logger.error(f"❌ Failed to generate signed URL for {s3_url}: {str(e)}")
        return s3_url  # Return original URL as fallback

def replace_s3_with_signed_markdown(text: str, signed_map: dict) -> str:
    """
    Replace raw S3 links with signed URL markdown format:
    [s3://signed-url (Page N)](signed-url)
    """
    for s3_url_key, signed_url in signed_map.items():
    # Extract page number from the key (it has " (Page: N)")
        page_match = re.search(r'Page:\s*(\d+)', s3_url_key, re.IGNORECASE)
        page = page_match.group(1) if page_match else "?"

        # Extract document name (folder before page_x.json)
        parts = s3_url_key.strip().split("/")
        doc_name = parts[-2] if parts else "Document"

        # Build markdown link in required format
        markdown_link = f"[{doc_name} - Page {page}]({signed_url}#page={page})"

        # Replace occurrences of the key in text with markdown
        pattern = (
            rf"\[.*?{re.escape(s3_url_key)}.*?\]\([^\)]*\)(?: \(Page: \d+\))?"
            rf"|{re.escape(s3_url_key)}(?: \(Page: \d+\))?"
        )
        match = re.match(r"^(.*?)(\s*\(Page:\s*\d+\))$", s3_url_key)
        base_url = match.group(1).strip()
        base_url = re.sub(
            r"/silver/pdf/oem/([^/]+)/page_\d+\.json$",
            r"/source_data/pdf/oem/\1.pdf",
            base_url
        )
        page_info = match.group(2).strip()
        text = re.sub(re.escape(f'[{base_url}]({base_url}) {page_info}'), markdown_link, text)
    
    return text

def create_signed_url_mapping(s3_url_dict: Dict[str, str], expiry_hours: int = 24) -> Dict[str, str]:
    """
    Create mapping of dict keys → signed URLs.
    Keys remain unchanged.
    Values (S3 URLs) get signed and replaced in the same dict.
    """
    mapping = {}
    successful_mappings = 0

    for key, s3_url in s3_url_dict.items():
        if s3_url and isinstance(s3_url, str) and s3_url.startswith('s3://'):
            try:
                signed_url = generate_signed_url_for_s3_path(s3_url, expiry_hours)
                key = key.replace("prodprod", "prod")
                key = re.sub(
                    r"/silver/pdf/oem/([^/]+)/page_\d+\.json$",
                    r"/source_data/pdf/oem/\1.pdf",
                    key
                )
                mapping[key] = signed_url
                if signed_url != s3_url:  # Only count if URL was actually signed
                    successful_mappings += 1
            except Exception as e:
                logger.error(f"❌ Failed to create signed URL for {s3_url}: {str(e)}")
                mapping[key] = s3_url  # fallback
        else:
            logger.debug(f"⚠️ Skipping invalid or non-S3 URL: {s3_url}")
            mapping[key] = s3_url

    logger.info(f"🔗 Created signed URL mapping: {successful_mappings}/{len(s3_url_dict)} URLs successfully signed")
    return mapping


def get_signed_url_stats(url_mapping: Dict[str, str]) -> Dict[str, int]:
    """Get statistics about signed URL mapping"""
    stats = {
        "total_urls": len(url_mapping),
        "signed_urls": 0,
        "fallback_urls": 0
    }
    
    for original, signed in url_mapping.items():
        if original != signed and signed.startswith('https://'):
            stats["signed_urls"] += 1
        else:
            stats["fallback_urls"] += 1
    
    return stats