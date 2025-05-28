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
    # def file_path(self, request, response=None, info=None, *, item=None):
    #     file_url_hash = hashlib.shake_256(request.url.encode()).hexdigest(5)
    #     file_doc_name = request.url.split("/")[-1]
    #     file_name = f"{file_url_hash}_{file_doc_name}"

    #     return file_name

    def file_path(self, request, response=None, info=None, *, item=None):
        return "full/" + PurePosixPath(urlparse_cached(request).path).name
    

