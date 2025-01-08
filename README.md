# Companies House Web Scraper

Web scraping tool to find URL and contact information from UK businesses based on Companies House data. This tool uses the CSV and webscrapes for information regarding the companies in the spreadsheet.

CSV files can be downloaded from the Companies House Advanced Search tool:

<https://find-and-update.company-information.service.gov.uk/advanced-search>

The program has a CLI. To run the code ensure csv is in same path directory and run terminal prompt:
```
python main.py
```


Required Python libraries:
- httpx
- bs4
- pandas
- numpy
- geopy
- tqdm
- googlesearch

As well as standard libraries:
- random
- time
- re
- sys
