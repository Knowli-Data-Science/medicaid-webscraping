# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
import boto3
import hashlib
import os
from pathlib import PurePosixPath
from scrapy.utils.httpobj import urlparse_cached
from scrapy.exceptions import IgnoreRequest
from botocore.errorfactory import ClientError
from urllib.parse import urlparse, unquote

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class MedscraperSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # maching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class MedscraperDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.
    def __init__(self, s3_bucket, s3_folder):
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.s3 = boto3.client('s3')

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(
            s3_bucket=crawler.settings.get('S3_BUCKET'),
            s3_folder=crawler.settings.get('S3_FOLDER')
        )
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        is_file = response.headers.get('Content-Type', b'').decode().lower()
        print("HERE IS FILE: ", is_file)
        print("RESPONSE: ", response.url)
        if not is_file or 'application' not in is_file:
            print("IS NOT FILE HERE")
            return response
        
        new_file_hash = hashlib.md5(response.body).hexdigest()

        file_key = f"{self.s3_folder}/{self.get_s3_key(response.url)}"

        if self.s3_file_exists(file_key):
            local_file_hash = self.get_s3_file_hash(file_key)
            if new_file_hash == local_file_hash:
                spider.logger.info(f"[S3 Middleware] File contents unchanged; Skipping file: {file_key}")
                raise IgnoreRequest
            
        return response
        
    def get_s3_key(self, url):
        path = urlparse(url).path
        return os.path.basename(unquote(path))
    
    def s3_file_exists(self, key):
        try:
            self.s3.head_object(Bucket=self.s3_bucket, Key=key)
            return True
        except ClientError as e:
            print(e.response)
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def get_s3_file_hash(self, key):
        file = self.s3.get_object(Bucket=self.s3_bucket, Key=key)
        body = file['Body'].read()
        print("FILE BODY: ", body)
        return hashlib.md5(body).hexdigest()
            
    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
