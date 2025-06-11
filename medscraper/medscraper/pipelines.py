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
    def __init__(self, store_uri, *args, **kwargs):
        super().__init__(store_uri, *args, **kwargs)
        self.s3 = boto3.client("s3")
        self.s3_bucket = None

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = super().from_crawler(crawler)
        pipeline.s3_bucket = crawler.settings.get("S3_BUCKET")
        pipeline._fingerprinter = crawler.request_fingerprinter
        return pipeline

    def get_media_requests(self, item, info):
        logger.info("HITTING MEDIA REQUESTS")
        for file_url in item.get("file_urls", []):
            yield scrapy.Request(file_url, meta={"item": item})

    def file_path(self, request, response=None, info=None, *, item=None):
        return "policy-docs/full/" + urlparse(request.url).path.split("/")[-1]

    def media_to_download(self, request, info, *, item=None):
        # Decides whether the file should be downloaded based on hash comparison.
        file_key = self.file_path(request, info=info, item=item)

        try:
            s3_file = self.s3.get_object(Bucket=self.s3_bucket, Key=file_key)
            existing_hash = hashlib.sha256(s3_file["Body"].read()).hexdigest()
            request.meta["existing_hash"] = existing_hash
            logger.info(f"[S3 File Pipeline] Existing file hash for {file_key}: {existing_hash}")
        except self.s3.exceptions.NoSuchKey:
            logger.info(f"[S3 File Pipeline] No existing file found for {file_key}, will download.")
            return True

        # Proceed with download to compare hashes after download
        return True

    def file_downloaded(self, response, request, info, *, item=None):
        # Compares the new hash to the existing one from S3
        logger.info("HITTING FILE DOWNLOADED")
        file_key = self.file_path(request, response=response, info=info, item=item)
        new_hash = hashlib.sha256(response.body).hexdigest()
        old_hash = request.meta.get("existing_hash")

        if old_hash and new_hash == old_hash:
            logger.info(f"[S3 File Pipeline] Skipping unchanged file: {file_key}")
            raise DropItem(f"[S3 File Pipeline] File unchanged: {file_key}")

        logger.info(f"[S3 File Pipeline] New or changed file: {file_key}")

        return {
            "url": request.url,
            "path": file_key,
            "checksum": new_hash,
        }

    

