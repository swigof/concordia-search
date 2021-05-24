concordia.ca webpage crawler, index builder, and queryer

Searches for pages related to covid and sustainability

# Requirements 
* bs4
* nltk
* scrapy
* protego

# Execution
> python P4.py

# Outputs
* blocks/      <-- The blocks from the SPIMI index building process
* crawled/     <-- The crawled webpage with IDs for filenames
* doclengths   <-- The lengths of the index documents
* IDtoLink.csv <-- A file mapping crawled IDs to urls
* index        <-- The built index
* returns.txt  <-- The returns from the queries on the index

# Results and Observations
* Query results are not very accurate, due to a number of limitations imposed to make the script realistically executable. 
* The full domain couldn't be crawled due to a significant number of useless and/or duplicate pages. Many more exclusion rules would be needed. 
* Postings lists in the index are limited to 50 entries each to reduce file size, causing information loss. 
* Targetting specific site portions or subdomains relating to research would get better query results.
