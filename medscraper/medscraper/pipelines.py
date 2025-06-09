# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import hashlib
from pathlib import PurePosixPath
from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline
from scrapy.utils.httpobj import urlparse_cached

class MedscraperPipeline(FilesPipeline):    
    def file_path(self, request, response=None, info=None, *, item=None):
        return "full/" + PurePosixPath(urlparse_cached(request).path).name
    

