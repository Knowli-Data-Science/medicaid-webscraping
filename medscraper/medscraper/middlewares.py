# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import scrapy
import requests
import os
import logging
from scrapy import signals
from scrapy.http import HtmlResponse
from dotenv import load_dotenv
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

# Env file with api key
ENV_PATH = ".env"
logger = logging.getLogger(__name__)

if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)
    print(f"✅ Loaded environment variables from {ENV_PATH}")
else:
    raise FileNotFoundError(f"🚨 .env file not found at {ENV_PATH}")


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
    def __init__(self):
        self.api_key = os.getenv('ZENROWS_API_KEY')

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware. Modifies the request object to send requests through ZenRows' API to handle User Agent rotation at scale
        
        # Set the url to the target page being requested
        target_url = request.url
        # Form a proxy url to go through ZenRows and allow it to handle user agent rotation/proxying
        proxy_url = (
            f"https://api.zenrows.com/v1/"
            f"?apikey={self.api_key}"
            f"&url={target_url}"
        )

        request._original_url = request.url  # Optional, if the script needs to recover after failure
        request.replace(url=proxy_url) # Set the request's url to the new proxy url 
        request.headers.pop('User-Agent', None)  # Let ZenRows API set the User-Agent tag
            
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
