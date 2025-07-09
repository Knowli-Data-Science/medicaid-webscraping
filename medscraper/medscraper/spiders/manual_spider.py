import scrapy
import re
import us.states
from medscraper.items import PolicyManualsPackage
from scrapy.loader import ItemLoader
from datetime import datetime

# Custom scrapy spider class ManualSpider; extracts the most recent Billing Provider Policy Manual document files from state healthcare
# sites, uploads the relevant/updated files, and collects and uploads metadata about the sites it visits and the files downloaded from
# each individual site
class ManualSpider(scrapy.Spider):
    name = "manuals"
    
    allowed_domains = [
        "aaaaspider.com",
        "ahca.myflorida.com",
        "flrules.org",
        "pamms.dhs.ga.gov",
        "www.kymmis.com",
        "www.tn.gov",
        "medicaid.alabama.gov",
        "medicaid.ms.gov",
        "www1.scdhhs.gov",
        "img1.scdhhs.gov",
        "www.nctracks.nc.gov",
        "www.dmas.virginia.gov"
    ]
    
    valid_base_urls = [
        # "https://aaaaspider.com",
        "https://ahca.myflorida.com/medicaid/rules",
        "https://pamms.dhs.ga.gov/dfcs/medicaid",
        "https://www.kymmis.com/kymmis",
        "https://www.tn.gov/tenncare/policy-guidelines/eligibility-policy",
        "https://medicaid.alabama.gov/content/Gated/7.6.1G_Provider_Manuals",
        "https://medicaid.ms.gov/eligibility-policy-and-procedures-manual",
        "http://www1.scdhhs.gov/mppm",
        "https://www.nctracks.nc.gov/content/public/providers",
        "https://www.dmas.virginia.gov/for-applicants/eligibility-guidance/eligibility-manual"
    ]
    
    state_dict = {
        "https://ahca.myflorida.com/": "Florida",
        "https://pamms.dhs.ga.gov/dfcs/": "Georgia",
        "https://www.kymmis.com/": "Kentucky",
        "https://www.tn.gov/": "Tennessee",
        "https://medicaid.alabama.gov/": "Alabama",
        "https://medicaid.ms.gov/": "Mississippi",
        "http://www1.scdhhs.gov/": "South Carolina",
        "https://www.nctracks.nc.gov/": "North Carolina",
        "https://www.dmas.virginia.gov/": "Virginia"
    }
    
    # Function for checking if the url being requested begins with one of the desired base paths
    def is_allowed_url(self, url):
        return any(url.startswith(prefix) for prefix in self.valid_base_urls)
    
    async def start(self):
        # State healthcare sites which the spider will begin crawling from, extracting policy documents as it goes
        urls = [
            # "https://aaaaspider.com",
            # "https://ahca.myflorida.com/medicaid/rules/adopted-rules-general-policies",
            # "https://pamms.dhs.ga.gov/dfcs/medicaid/",
            # "https://www.kymmis.com/kymmis/Provider%20Relations/billingInst.aspx",
            # "https://www.tn.gov/tenncare/policy-guidelines/eligibility-policy.html",
            "https://medicaid.alabama.gov/content/Gated/7.6.1G_Provider_Manuals/7.6.1.2G_Apr2025.aspx",
            # "https://medicaid.ms.gov/eligibility-policy-and-procedures-manual/",
            # "http://www1.scdhhs.gov/mppm/",
            # "https://www.nctracks.nc.gov/content/public/providers/provider-manuals.html",
            # "https://www.dmas.virginia.gov/for-applicants/eligibility-guidance/eligibility-manual/"
        ]
        
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Load in the custom scrapy item class PolicyManualsPackage, representing a package of files and its metadata downloaded from a particular url
        # and the response passed in from the spider
        loader = ItemLoader(item=PolicyManualsPackage(), response=response)

        # Get every link on the site
        all_links = response.css('a::attr(href)').getall()

        # Parse the entire site's text, search for which state the package is associated with, and load it into the item
        # site_text = response.text
        # loader.add_value("package_state", "Invalid")
        # for state in us.states.STATES:
        #     if state.name in site_text:
        #         loader.replace_value("package_state", state.name)
        
        # Check the current link's prefix against the stored dict to find its matching associated state
        loader.add_value("package_state", "Invalid")
        for state_url in self.state_dict.keys():
            if response.url.startswith(state_url):
                loader.replace_value("package_state", self.state_dict[state_url])

        # Collect every link on the site leading to either a .pdf or a .docx file, and retrieve/store the full url
        file_urls = []
        for link in all_links:
            full_link = response.urljoin(link)
            if re.search(r'\.pdf$', link, re.IGNORECASE) or re.search(r'\.docx$', link, re.IGNORECASE):
                file_urls.append(full_link)
            
            if self.is_allowed_url(full_link): 
                yield scrapy.Request(full_link, self.parse)
                
        # Convert the list of links to a dict to filter out duplicates, convert back to a list to preserve the lexicographic order,
        # then load all the links and the link count into the item
        file_urls = list(dict.fromkeys(file_urls))
        loader.add_value("file_urls", file_urls)
        loader.add_value("package_file_count", len(file_urls))

        # Generate a timestamp for when these files and metadata were retrieved, and load them into the item
        timestamp = datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
        loader.add_value("package_retrieval_date", timestamp)
        loader.add_value("package_last_checked", timestamp)

        # Load the current site path the spider is on into the item
        loader.add_value("package_site_path", response.url)

        # Log success for downloading the item, print the timestamp, and populate the item with all collected data
        self.logger.info(f"Package downloaded. Timestamp: {timestamp}")
        file_package_item = loader.load_item()

        # Return the fully populated file package item, which uploads all collected policy documents to s3 bucket
        yield file_package_item

