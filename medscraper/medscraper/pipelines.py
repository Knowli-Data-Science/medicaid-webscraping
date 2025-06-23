# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import hashlib
import boto3
import scrapy
import logging
from urllib.parse import urlparse
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem

logger = logging.getLogger(__name__)

class MedscraperPipeline(FilesPipeline):
    def __init__(self, store_uri, download_func=None, settings=None, *args, **kwargs):
        super().__init__(store_uri, download_func, settings, *args, **kwargs)
        self.s3 = None
        self.s3_bucket = None

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = super().from_crawler(crawler)
        pipeline.s3 = boto3.client('s3')
        pipeline.s3_bucket = crawler.settings.get('S3_BUCKET')
        return pipeline

    def get_media_requests(self, item, info):
        for file_url in item.get("file_urls", []):
            yield scrapy.Request(file_url, meta={"item": item})

    def file_path(self, request, response=None, info=None, *, item=None):
        return "policy-docs/full/" + urlparse(request.url).path.split("/")[-1]

    def media_downloaded(self, response, request, info, *, item=None):
        result = super().media_downloaded(response, request, info, item=item)
        logger.info(f"[S3 File Pipeline] File storage result: {result}")
        return result

    def file_downloaded(self, response, request, info, *, item=None):
        # Compares the new hash to the existing one from S3
        logger.info(f"[S3 File Pipeline] Downloaded {request.url} with status {response.status}")
        
        file_key = self.file_path(request, response=response, info=info, item=item)
        new_hash = hashlib.sha256(response.body).hexdigest()

        try:
            s3_file = self.s3.get_object(Bucket=self.s3_bucket, Key=file_key)
            existing_hash = hashlib.sha256(s3_file["Body"].read()).hexdigest()
            logger.info(f"[S3 File Pipeline] File Hashes for {file_key}:\n Existing - {existing_hash}\n New - {new_hash}")

            if new_hash == existing_hash:
                logger.info(f"[S3 File Pipeline] Skipping unchanged file: {file_key}")
                item["file_urls"].remove(request.url)
                raise DropItem(f"[S3 File Pipeline] File unchanged: {file_key}")
            else:
                logger.info(f"[S3 File Pipeline] Changed file {file_key}; Downloading...")
                item["file_urls"].append(request.url)
                content_type = self._get_content_type(file_key)            
                self.s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=file_key,
                    Body=response.body,
                    ContentType=content_type
                )
        except self.s3.exceptions.NoSuchKey:
            logger.info(f"[S3 File Pipeline] New file {file_key}; Downloading...")
            item["file_urls"].append(request.url)
            content_type = self._get_content_type(file_key)            
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=file_key,
                Body=response.body,
                ContentType=content_type
            )

        return new_hash
    
    def media_failed(self, failure, request, info, *, item=None):
        logger.error(f"Media failed for {request.url}: {failure}")
    
    def _get_content_type(self, file_key):
        """Determine content type based on file extension."""
        extension = file_key.lower().split('.')[-1]
        content_types = {
            'pdf': 'application/pdf',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'txt': 'text/plain',
            'csv': 'text/csv',
            'html': 'text/html',
            'htm': 'text/html',
            'json': 'application/json',
            'xml': 'application/xml',
        }
        return content_types.get(extension, 'application/octet-stream')


    

