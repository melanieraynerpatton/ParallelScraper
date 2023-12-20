# ParallelScraper
Program that scrapes Refin.com in parallel using Python

Uses Python's multiprocessing library to scrape Redfin.com in parallel. Using 10 cores, the program runs in about 36 hours. 
The data is gathered from Redfin URLs with zip codes from the lower 48 states appended to them (text file containing zips is attached).
The program will gather all the zip codes in a list, then create a list of URLs to scrape. The list will be broken up into size 100 chunks to preserve memory and efficiency.
Then a multiprocessing Pool object is created, which will spawn 10 subprocesses. Each subprocess will scrape 10 URLs, and the pool will be refreshed after each URL block.
The data from each URL is flattened and placed in a CSV. Each CSV will be concatenated into one using another Process object and a Dask Dataframe. 
