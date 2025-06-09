from pathlib import Path

import io
import scrapy
import re
import ast
import us.states
import pandas as pd
import boto3
from medscraper.items import PolicyManualsPackage
from scrapy.loader import ItemLoader
from datetime import datetime
from botocore.errorfactory import ClientError

# S3 Bucket setting
S3_BUCKET = 'webscraped-docs-test'

# Custom scrapy spider class ManualSpider; extracts the most recent Billing Provider Policy Manual document files from state healthcare
# sites, uploads the relevant/updated files, and collects and uploads metadata about the sites it visits and the files downloaded from
# each individual site
class ManualSpider(scrapy.Spider):
    name = "manuals"

    # Helper function, fetching each metadata dataframe if it exists in the s3 bucket, and creating new ones if not
    def fetch_doc_data(file_path):
        s3 = boto3.client('s3')

        try:
            # Fetch the requested file's dataframe from the s3 bucket if it exists
            obj = s3.get_object(Bucket=S3_BUCKET, Key=file_path)
            table = pd.read_csv(obj['Body'])
            return table
        except ClientError as e:
            print(e.response)

            # Otherwise, form new dataframes in the correct configuration
            return pd.DataFrame(columns=["file_urls", "package_state", "package_site_path", "package_file_count", "package_retrieval_date", "package_last_checked"])
                
    def insert_or_update(df: pd.DataFrame, new_record: pd.Series) -> pd.DataFrame:
        ignore_cols = {"package_retrieval_date", "package_last_checked"}
        compare_cols = [col for col in df.columns if col not in ignore_cols]

        def normalize(value):
            if isinstance(value, str):
                # Try to parse list-like strings into real lists
                try:
                    parsed = ast.literal_eval(value)
                    if isinstance(parsed, list):
                        return tuple(parsed)
                except (ValueError, SyntaxError):
                    pass
                return value.strip()
            if isinstance(value, list):
                return tuple(value)
            return value
        
        compare_df = df[compare_cols].map(normalize)
        compare_record = pd.Series({k: normalize(new_record[k]) for k in compare_cols})

        mask = (compare_df == compare_record).all(axis=1)

        # for idx, row in df[compare_cols].iterrows():
        #     print(f"\n--- Row {idx} ---")
        #     for col in compare_cols:
        #         df_val = row[col]
        #         record_val = new_record[col]
        #         print(f"Column: {col}")
        #         print(f"  DataFrame value: {df_val!r} (type: {type(df_val)})")
        #         print(f"  New record value: {record_val!r} (type: {type(record_val)})")

        if mask.any():
            df.loc[mask, "package_last_checked"] = new_record["package_last_checked"]
        else:
            df.loc[len(df)] = new_record
        
        return df
    
    async def start(self):
        # State healthcare sites which the spider will begin crawling from, extracting policy documents as it goes
        urls = [
            "https://aaaaspider.com",
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
        allowed_domains = [
            "https://aaaaspider.com",
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
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Load in the custom scrapy item class PolicyManualsPackage, representing a package of files and its metadata downloaded from a particular url
        # and the response passed in from the spider
        loader = ItemLoader(item=PolicyManualsPackage(), response=response)

        # Get every link on the site
        all_links = response.css('a::attr(href)').getall()

        # Parse the entire site's text, search for which state the package is associated with, and load it into the item
        site_text = response.text
        loader.add_value("package_state", "Invalid")
        for state in us.states.STATES:
            if state.name in site_text:
                loader.replace_value("package_state", state.name)

        # Collect every link on the site leading to either a .pdf or a .docx file, and retrieve/store the full url
        file_urls = []
        for link in all_links:
            if re.search(r'\.pdf$', link, re.IGNORECASE) or re.search(r'\.docx$', link, re.IGNORECASE):
                file_urls.append(response.urljoin(link))
                
        # Convert the list of links to a dict to filter out duplicates, convert back to a list to preserve the lexicographic order,
        # then load all the links and the link count into the item
        file_urls = list(dict.fromkeys(file_urls))
        loader.add_value("file_urls", file_urls)
        loader.replace_value("package_file_count", len(file_urls))

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
        master_table = ManualSpider.fetch_doc_data("doc-data/master_table.csv")
        
        # Create new dataframe records from currently collected metadata in the item
        new_record = pd.Series(dict(file_package_item))

        # Insert new file package metadata records into appropriate tables, and print the resulting tables to the console for confirmation
        print("MASTER TABLE BEFORE: ", master_table)

        master_table = ManualSpider.insert_or_update(master_table, new_record)

        print("MASTER TABLE AFTER: ", master_table)
        
        state_table = master_table.groupby(by = ['package_state', 'package_site_path']).agg({'package_file_count': 'sum'})
        file_count_table = master_table.groupby('package_state').agg({'package_file_count': 'sum'})
        
        # Finally, upload new file package metadata dataframes to s3 bucket
        try:
            s3 = boto3.client('s3')
            for file, key in zip([master_table, state_table, file_count_table], ["doc-data/master_table.csv", "doc-data/state_table.csv", "doc-data/file_count_table.csv"]):
                csv_buffer = io.StringIO()
                file.to_csv(csv_buffer, index=False)

                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=key,
                    Body=csv_buffer.getvalue(),
                    ContentType="text/csv"
                )
        except ClientError as e:
            print(e.response)

        # Return the fully populated file package item, which uploads all collected policy documents to s3 bucket
        return file_package_item

