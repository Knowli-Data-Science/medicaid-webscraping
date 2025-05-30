from pathlib import Path

import scrapy
import re
import us.states
import pandas as pd
from medscraper.items import PolicyManualsPackage
from scrapy.loader import ItemLoader
from datetime import datetime

class ManualSpider(scrapy.Spider):
    name = "manuals"

    async def start(self):
        urls = [
            "https://ahca.myflorida.com/medicaid/rules/adopted-rules-general-policies",
            # "https://pamms.dhs.ga.gov/dfcs/medicaid/",
            # "https://www.kymmis.com/kymmis/Provider%20Relations/billingInst.aspx",
            "https://www.tn.gov/tenncare/policy-guidelines/eligibility-policy.html",
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

        site_text = response.text
        for state in us.states.STATES:
            if state.name in site_text:
                loader.add_value("package_state", state.name)

        file_urls = []
        for link in all_links:
            if re.search(r'\.pdf$', link, re.IGNORECASE) or re.search(r'\.docx$', link, re.IGNORECASE):
                file_urls.append(response.urljoin(link))
                
        file_urls = list(dict.fromkeys(file_urls))
        loader.add_value("file_urls", file_urls)
        loader.replace_value("package_file_count", len(file_urls))

        timestamp = datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
        loader.add_value("package_download_date", timestamp)

        loader.add_value("package_site_path", response.url)

        self.logger.info(f"Package downloaded. Timestamp: {timestamp}")
        file_package_item = loader.load_item()
        
        master_table = pd.read_csv("medscraper/policy-docs/data/master_table.csv")
        state_table = pd.read_csv("medscraper/policy-docs/data/state_table.csv")
        file_count_table = pd.read_csv("medscraper/policy-docs/data/file_count_table.csv")
        
        new_master_record = pd.DataFrame([file_package_item])
        new_state_record = pd.DataFrame(new_master_record.loc[:, ["package_state", "package_site_path", "package_file_count"]])
        new_filecount_record = pd.DataFrame(new_master_record.loc[:, ["package_state", "package_file_count"]])
        
        master_table = pd.concat([master_table, new_master_record], ignore_index=True)
        state_table = pd.concat([state_table, new_state_record], ignore_index=True)
        file_count_table = pd.concat([file_count_table, new_filecount_record], ignore_index=True)
        
        print("Main DataFrame Table: \n", master_table.to_string())
        print("State Link Table: \n", state_table.to_string())
        print("State File Count Table: \n", file_count_table.to_string())
        
        master_table.to_csv("medscraper/policy-docs/data/master_table.csv")
        state_table.to_csv("medscraper/policy-docs/data/state_table.csv")
        file_count_table.to_csv("medscraper/policy-docs/data/file_count_table.csv")
        return file_package_item

