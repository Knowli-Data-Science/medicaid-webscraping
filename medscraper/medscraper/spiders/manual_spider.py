from pathlib import Path

import io
import scrapy
import re
import ast
import us.states
import pandas as pd
import boto3
import logging
from medscraper.items import PolicyManualsPackage
from scrapy.loader import ItemLoader
from datetime import datetime
from botocore.errorfactory import ClientError

# S3 Bucket setting
S3_BUCKET = 'webscraped-docs-test'

logger = logging.getLogger(__name__)

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

    # Helper function, fetching the main metadata dataframe if it exists in the s3 bucket, and creating a new one if not
    # def fetch_doc_data(self, file_path):
    #     s3 = boto3.client('s3')

    #     try:
    #         # Fetch the requested file's dataframe from the s3 bucket if it exists
    #         obj = s3.get_object(Bucket=S3_BUCKET, Key=file_path)
    #         table = pd.read_csv(obj['Body'])
    #         return table
    #     except ClientError as e:
    #         print(e.response)

    #         # Otherwise, form new dataframes in the correct configuration
    #         return pd.DataFrame(columns=["file_urls", "package_state", "package_site_path", "package_file_count", "package_retrieval_date", "package_last_checked"])
    
    # Function for inserting new records, or updating previous records after sanitization, and prevention of duplicate records to the dataframe 
    # def insert_or_update(self, df: pd.DataFrame, new_record: pd.Series) -> pd.DataFrame:    
    #     # Helper function to sanitize incoming data from dataframes/series
    #     def normalize(value):
    #         if isinstance(value, str):
    #             # Try to parse list-like strings into real lists
    #             try:
    #                 parsed = ast.literal_eval(value)
    #                 if isinstance(parsed, list):
    #                     return tuple(parsed)
    #             except (ValueError, SyntaxError):
    #                 pass
    #             return value.strip()
    #         if isinstance(value, list):
    #             return tuple(value)
    #         return value
        
    #     # Create a set of which columns to ignore and which to consider when making comparisons;
    #     # ignore timestamps, and create a list of every other column to compare between main table and new record
    #     ignore_cols = {"package_retrieval_date", "package_last_checked"}
    #     compare_cols = [col for col in df.columns if col not in ignore_cols]

    #     # Take only the considered columns from the table and new record, and normalize them
    #     normalized_df = df[compare_cols].map(normalize)
    #     normalized_record = pd.Series({k: normalize(new_record[k]) for k in compare_cols})

    #     # Get only the rows from the table containing any of the file URLs present in the new record
    #     file_mask = normalized_df[normalized_df['file_urls'].apply(lambda x: bool(set(x).intersection(set(normalized_record['file_urls']))))]
        
    #     # for idx, row in df[compare_cols].iterrows():
    #     #     print(f"\n--- Row {idx} ---")
    #     #     for col in compare_cols:
    #     #         df_val = row[col]
    #     #         record_val = new_record[col]
    #     #         print(f"Column: {col}")
    #     #         print(f"  DataFrame value: {df_val!r} (type: {type(df_val)})")
    #     #         print(f"  New record value: {record_val!r} (type: {type(record_val)})")

    #     # If there exist any rows from the table which contain a file URL matching one in the new record,
    #     if not file_mask.empty:
    #         # Create a new set containing every file URL from every row that was retrieved
    #         matched_rows = set().union(*file_mask['file_urls'])
    #         # Make a set out of the new record's file URLs
    #         nr_files = set(new_record['file_urls'])
    #         # Subtract the matched files from the new record's set of file URLs, and whatever is left is new
    #         unique_files = nr_files - matched_rows
                    
    #         # If there are any unique files left,
    #         if unique_files:
    #             # Set the normalized record's list of files to the list of unique files, for comparison
    #             normalized_record['file_urls'] = tuple(unique_files)
                
    #             # Retrieve the row from the table for which everything is exactly the same as the new, normalized, file-duplicate sanitized record
    #             time_mask = (normalized_df == normalized_record).all(axis=1)
                
    #             # If the sanitized record already exists, simply update its timestamp
    #             if time_mask.any():
    #                 df.loc[time_mask, "package_last_checked"] = new_record["package_last_checked"]
    #             else:
    #             # Otherwise, the record is brand new, so set the new record's file list to the list of unique files, and add it to the table
    #                 new_record['file_urls'] = list(unique_files)
    #                 df.loc[len(df)] = new_record
    #         else:
    #             # As this record has no unique files, check if this exact record already exists
    #             time_mask = (normalized_df == normalized_record).all(axis=1)
                
    #             # If so, simply update its timestamp
    #             if time_mask.any():
    #                 df.loc[time_mask, "package_last_checked"] = new_record["package_last_checked"]
    #             else:
    #             # Otherwise, if the new record contains all duplicate files, but does not already exist in the table, 
    #             # this record contains no new information, so simply update every record's timestamp which contains a file in this record
    #                 file_mask = file_mask.all(axis=1)
    #                 df.loc[file_mask, "package_last_checked"] = new_record["package_last_checked"]
    #     else:
    #         # Otherwise, only check if the record already exists
    #         time_mask = (normalized_df == normalized_record).all(axis=1)
            
    #         # If so, update its timestamp
    #         if time_mask.any():
    #             df.loc[time_mask, "package_last_checked"] = new_record["package_last_checked"]
    #         else:
    #         # If the new record neither contains duplicate files, nor already exists in the table, simply add it to the table
    #             df.loc[len(df)] = new_record
        
    #     # Return the newly updated table
    #     return df
    
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

        # Fetch current policy document metadata from s3 bucket, or create new dataframes for them if they dont exist
        # master_table = self.fetch_doc_data("doc-data/master_table.csv")
        
        # # Create new dataframe records from currently collected metadata in the item
        # new_record = pd.Series(dict(file_package_item))

        # # Insert new file package metadata records into appropriate tables, and print the resulting tables to the console for confirmation
        # master_table = self.insert_or_update(master_table, new_record)

        # state_table = master_table.groupby(by = ['package_state', 'package_site_path']).agg({'package_file_count': 'sum'})
        # file_count_table = master_table.groupby('package_state').agg({'package_file_count': 'sum'})

        # master_table.to_csv("medscraper/doc-data/master_table.csv")
        # state_table.to_csv("medscraper/doc-data/state_table.csv")
        # file_count_table.to_csv("medscraper/doc-data/file_count_table.csv")
        
        # # Finally, upload new file package metadata dataframes to s3 bucket
        # try:
        #     s3 = boto3.client('s3')
        #     for file, key in zip([master_table, state_table, file_count_table], ["doc-data/master_table.csv", "doc-data/state_table.csv", "doc-data/file_count_table.csv"]):
        #         csv_buffer = io.StringIO()
        #         file.to_csv(csv_buffer, index=False)

        #         s3.put_object(
        #             Bucket=S3_BUCKET,
        #             Key=key,
        #             Body=csv_buffer.getvalue(),
        #             ContentType="text/csv"
        #         )
        # except ClientError as e:
        #     print(e.response) 

        # Return the fully populated file package item, which uploads all collected policy documents to s3 bucket
        yield file_package_item

