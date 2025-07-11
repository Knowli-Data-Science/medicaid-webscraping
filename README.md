# Medicaid Policy Document Webscraper -- Medscraper

   ### 
   Primary Developer: Ian Estevez, Software Developer Intern 2025

   ###
   Description:
   Custom Scrapy script that extracts the most recent Billing Provider Policy Manual document files from state healthcare sites, hashes and checks the contents of each file to check for updates, uploads the relevant/updated files, and collects and uploads metadata about the sites it visits and the files downloaded from each individual site. 

   ###
   How to compile & run project:
   Ensure you are within the inner medscraper project folder by running
   ```bash
      cd medscraper
   ```
   inside the medicaid-webscraping directory.
  
   Then, simply run
   ```bash
      scrapy crawl manuals
   ```
   to run the script.
   
   ###
   Important Dependencies:
   Scrapy, Python3, Boto3, Botocore, Pandas, Hashlib, Urllib
