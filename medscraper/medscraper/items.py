# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
from scrapy.item import Item, Field
from itemloaders.processors import TakeFirst

class PolicyManualsPackage(Item):
    # define the fields for your item here like:
    file_urls = Field()
    files = Field()
    package_retrieval_date = Field(output_processor=TakeFirst())
    package_last_checked = Field(output_processor=TakeFirst())
    package_site_path = Field(output_processor=TakeFirst())
    package_file_count = Field(output_processor=TakeFirst())
    package_state = Field(output_processor=TakeFirst())
    pass
