# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
import io
import hashlib
import boto3
import scrapy
import logging
import ast
import pandas as pd
from urllib.parse import urlparse
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem
from botocore.errorfactory import ClientError

logger = logging.getLogger(__name__)

class MedscraperPipeline(FilesPipeline):
    def __init__(self, store_uri, download_func=None, settings=None, *args, **kwargs):
        super().__init__(store_uri, download_func, settings, *args, **kwargs)
        self.s3_client = None
        self.s3_bucket = None

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = super().from_crawler(crawler)
        pipeline.s3_client = boto3.client('s3')
        pipeline.s3_bucket = crawler.settings.get('S3_BUCKET')
        return pipeline

    def get_media_requests(self, item, info):
        for file_url in item.get("file_urls", []):
            yield scrapy.Request(file_url, meta={"item": item})

    def file_path(self, request, response=None, info=None, *, item=None):
        return "policy-docs/full/" + urlparse(request.url).path.split("/")[-1]

    def media_downloaded(self, response, request, info, *, item=None):
        item['package_file_count'] = len(item['file_urls'])
        self.upload_metadata(item)
        result = super().media_downloaded(response, request, info, item=item)
        return result

    def file_downloaded(self, response, request, info, *, item=None):
        # Compares the new hash to the existing one from S3
        logger.info("HITTING FILE DOWNLOADED:")
        logger.info(f"[S3 File Pipeline] Downloaded {request.url} with status {response.status}")
        
        # Get the path of the key in the s3 bucket, and hash the newly scraped file's contents
        file_key = self.file_path(request, response=response, info=info, item=item)
        new_hash = hashlib.sha256(response.body).hexdigest()

        try:
            # Retrieve the local version of the file from the s3 bucket, and hash its contents
            s3_file = self.s3_client.get_object(Bucket=self.s3_bucket, Key=file_key)
            existing_hash = hashlib.sha256(s3_file["Body"].read()).hexdigest()
            logger.info(f"[S3 File Pipeline] File Hashes for {file_key}:\n Existing - {existing_hash}\n New - {new_hash}")

            # If the hashes match, the file contents have not changed, so drop the file from the package
            if new_hash == existing_hash:
                logger.info(f"[S3 File Pipeline] Skipping unchanged file: {file_key}")
                item["file_urls"].remove(request.url)
                raise DropItem(f"[S3 File Pipeline] File unchanged: {file_key}")
            else:
                # Otherwise, the file contents have changed, so re-download the file
                logger.info(f"[S3 File Pipeline] Changed file {file_key}; Downloading...")
                item["file_urls"].append(request.url)
                content_type = self._get_content_type(file_key)            
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=file_key,
                    Body=response.body,
                    ContentType=content_type
                )
        except self.s3_client.exceptions.NoSuchKey:
            # Otherwise, if the file is not found in the s3 bucket, it's new, so upload to AWS
            logger.info(f"[S3 File Pipeline] New file {file_key}; Downloading...")
            item["file_urls"].append(request.url)
            content_type = self._get_content_type(file_key)            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=file_key,
                Body=response.body,
                ContentType=content_type
            )

        return new_hash
    
    def media_failed(self, failure, request, info, *, item=None):
        logger.error(f"Media failed for {request.url}: {failure}")
    
    def _get_content_type(self, file_key):
        """Determine content type based on file extension."""
        extension = file_key.lower().split('.')[-1]
        content_types = {
            'pdf': 'application/pdf',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'txt': 'text/plain',
            'csv': 'text/csv',
            'html': 'text/html',
            'htm': 'text/html',
            'json': 'application/json',
            'xml': 'application/xml',
        }
        return content_types.get(extension, 'application/octet-stream')
    
    # Helper function, fetching the main metadata dataframe if it exists in the s3 bucket, and creating a new one if not
    def fetch_doc_data(self, file_path):
        try:
            # Fetch the requested file's dataframe from the s3 bucket if it exists
            obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=file_path)
            table = pd.read_csv(obj['Body'])
            return table
        except ClientError as e:
            print(e.response)

            # Otherwise, form new dataframes in the correct configuration
            return pd.DataFrame(columns=["file_urls", "package_state", "package_site_path", "package_file_count", "package_retrieval_date", "package_last_checked"])
        
    # Function for inserting new records, or updating previous records after sanitization, and prevention of duplicate records to the dataframe 
    def insert_or_update(self, df: pd.DataFrame, new_record: pd.Series) -> pd.DataFrame:    
        # Helper function to sanitize incoming data from dataframes/series
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
        
        # Create a set of which columns to ignore and which to consider when making comparisons;
        # ignore timestamps, and create a list of every other column to compare between main table and new record
        ignore_cols = {"package_retrieval_date", "package_last_checked"}
        compare_cols = [col for col in df.columns if col not in ignore_cols]

        # Take only the considered columns from the table and new record, and normalize them
        normalized_df = df[compare_cols].map(normalize)
        normalized_record = pd.Series({k: normalize(new_record[k]) for k in compare_cols})

        # Get only the rows from the table containing any of the file URLs present in the new record
        file_mask = normalized_df[normalized_df['file_urls'].apply(lambda x: bool(set(x).intersection(set(normalized_record['file_urls']))))]
        
        # for idx, row in df[compare_cols].iterrows():
        #     print(f"\n--- Row {idx} ---")
        #     for col in compare_cols:
        #         df_val = row[col]
        #         record_val = new_record[col]
        #         print(f"Column: {col}")
        #         print(f"  DataFrame value: {df_val!r} (type: {type(df_val)})")
        #         print(f"  New record value: {record_val!r} (type: {type(record_val)})")

        # If there exist any rows from the table which contain a file URL matching one in the new record,
        if not file_mask.empty:
            # Create a new set containing every file URL from every row that was retrieved
            matched_rows = set().union(*file_mask['file_urls'])
            # Make a set out of the new record's file URLs
            nr_files = set(new_record['file_urls'])
            # Subtract the matched files from the new record's set of file URLs, and whatever is left is new
            unique_files = nr_files - matched_rows
                    
            # If there are any unique files left,
            if unique_files:
                # Set the normalized record's list of files to the list of unique files, for comparison
                normalized_record['file_urls'] = tuple(unique_files)
                
                # Retrieve the row from the table for which everything is exactly the same as the new, normalized, file-duplicate sanitized record
                time_mask = (normalized_df == normalized_record).all(axis=1)
                
                # If the sanitized record already exists, simply update its timestamp
                if time_mask.any():
                    df.loc[time_mask, "package_last_checked"] = new_record["package_last_checked"]
                else:
                # Otherwise, the record is brand new, so set the new record's file list to the list of unique files, and add it to the table
                    new_record['file_urls'] = list(unique_files)
                    new_record['package_file_count'] = len(list(unique_files))
                    logger.info(f"NEW RECORD INSERTED: {new_record}")
                    df.loc[len(df)] = new_record
            else:
                # As this record has no unique files, check if this exact record already exists
                time_mask = (normalized_df == normalized_record).all(axis=1)
                
                # If so, simply update its timestamp
                if time_mask.any():
                    df.loc[time_mask, "package_last_checked"] = new_record["package_last_checked"]
                else:
                # Otherwise, if the new record contains all duplicate files, but does not already exist in the table, 
                # this record contains no new information, so simply update every record's timestamp which contains a file in this record
                    file_mask = file_mask.all(axis=1)
                    df.loc[file_mask, "package_last_checked"] = new_record["package_last_checked"]
        else:
            # Otherwise, only check if the record already exists
            time_mask = (normalized_df == normalized_record).all(axis=1)
            
            # If so, update its timestamp
            if time_mask.any():
                df.loc[time_mask, "package_last_checked"] = new_record["package_last_checked"]
            else:
            # If the new record neither contains duplicate files, nor already exists in the table, simply add it to the table
                df.loc[len(df)] = new_record
        
        # Return the newly updated table
        return df
    
    def upload_metadata(self, item):
        # Fetch current policy document metadata from s3 bucket, or create new dataframes for them if they dont exist
        logger.info("HITTING UPLOAD METADATA IN PIPELINE:")
        master_table = self.fetch_doc_data("doc-data/master_table.csv")
        
        # Create new dataframe records from currently collected metadata in the item
        new_record = pd.Series(dict(item))

        # Insert new file package metadata records into appropriate tables, and print the resulting tables to the console for confirmation
        master_table = self.insert_or_update(master_table, new_record)

        state_table = master_table.groupby(by = ['package_state', 'package_site_path']).agg({'package_file_count': 'sum'})
        file_count_table = master_table.groupby('package_state').agg({'package_file_count': 'sum'})

        master_table.to_csv("medscraper/doc-data/master_table.csv")
        state_table.to_csv("medscraper/doc-data/state_table.csv")
        file_count_table.to_csv("medscraper/doc-data/file_count_table.csv")
        
        # Finally, upload new file package metadata dataframes to s3 bucket
        try:
            for file, key in zip([master_table, state_table, file_count_table], ["doc-data/master_table.csv", "doc-data/state_table.csv", "doc-data/file_count_table.csv"]):
                csv_buffer = io.StringIO()
                file.to_csv(csv_buffer, index=False)

                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=key,
                    Body=csv_buffer.getvalue(),
                    ContentType="text/csv"
                )
        except ClientError as e:
            print(e.response) 

    

