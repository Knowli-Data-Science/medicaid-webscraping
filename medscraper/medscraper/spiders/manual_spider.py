from pathlib import Path

import scrapy
import re
from medscraper.items import PolicyManualsPackage
from scrapy.loader import ItemLoader
from datetime import datetime

class ManualSpider(scrapy.Spider):
    name = "manuals"

    async def start(self):
        urls = [
            "https://knowlids.sharepoint.com/sites/KnowliDataScience/SiteAssets/Forms/AllItems.aspx",
            # "https://ahca.myflorida.com/medicaid/rules/adopted-rules-general-policies",
            # "https://pamms.dhs.ga.gov/dfcs/medicaid/",
            # "https://www.kymmis.com/kymmis/Provider%20Relations/billingInst.aspx",
            # "https://www.tn.gov/tenncare/policy-guidelines/eligibility-policy.html",
            # "https://medicaid.alabama.gov/content/Gated/7.6.1G_Provider_Manuals/7.6.1.2G_Apr2025.aspx",
            # "https://medicaid.ms.gov/eligibility-policy-and-procedures-manual/",
            # "http://www1.scdhhs.gov/mppm/",
            # "https://www.nctracks.nc.gov/content/public/providers/provider-manuals.html",
            # "https://www.dmas.virginia.gov/for-applicants/eligibility-guidance/eligibility-manual/"
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        loader = ItemLoader(item=PolicyManualsPackage(), response=response)
        all_links = response.css('a::attr(href)').getall()
        # docs = []

        for link in all_links:
            if re.search(r'\.pdf$', link, re.IGNORECASE):
                # docs.append(response.urljoin(link))
                loader.add_value("file_urls", response.urljoin(link))
            elif re.search(r'\.docx$', link, re.IGNORECASE):
                # docs.append(response.urljoin(link))
                loader.add_value("file_urls", response.urljoin(link))
                
        timestamp = datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
        loader.add_value("package_download_date", timestamp)

        # self.logger.info(f"LINKS: {docs}")
        self.logger.info(f"Package downloaded. Timestamp: {timestamp}")
        return loader.load_item()


