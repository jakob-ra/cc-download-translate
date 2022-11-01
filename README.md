# CommonCrawl Downloader

[CommonCrawl](https://commoncrawl.org/the-data/get-started/) is a nonprofit organization that crawls the web and freely provides its archives and datasets to the public. The Common Crawl corpus contains petabytes of data collected since 2013. It contains monthly updates of raw web page data, extracted metadata and text extractions that are hosted on Amazon Web Services' (AWS) S3 storage servers located in the US-East-1 (Northern Virginia) AWS Region.

This script was written with the purpose of downloading and processing the raw web page data for a user-provided list of domain names (e.g. apple.com, walmart.com, microsoft.com). Since each crawl is saved as a 100TB+ WARC file, it is not practibable to download and process the files whole. Instead, we get the byte range within the WARC file where a specific subpage is stored by querying the CommonCrawl Index via Athena. Using the byte range, the raw html of a webpage is downloaded and parsed into clear text. Then all passages around mentions of a user-provided list of keywords are extracted. The passages, along with information about their source are uploaded in batches of csv files to S3. The output files can then be downloaded or further processed. The output files have the following format:

![image](https://user-images.githubusercontent.com/49194118/199245335-a00f27ad-01e4-470b-8a06-4f06a8efd4cb.png)

By using Fargate spot instances the processing is cheap (in case a task is interrupted, it is just re-attempted). Also transfer speed is maximal because we access the CommonCrawl data from the same AWS region where it is hosted.

All input files should be csv. 

*Warning:* urls should not contain "https" or "www." upfront.

**Parameters** </font> 
* n_subpages: number of shortest subpages that are selected for each website address in the output table. 
* url_keywords: list of keywords that if any of them exists in a url address, that url is selected in the output table. 
* crawl: this parameter determines the desired timeframe. CRAWL-NAME-YYYY-WW – The name of the crawl and year + week it was initiated. (... OR crawl = 'CC-MAIN-2020-24' OR crawl = 'CC-MAIN-2020-50')
 


The resulting table contains the address of all the historical subpages of the urls in the input file, for the selected timeframe.   
* select_subpages: The merged table created through the inner_join function is very large. Therefore, we only select the subpages that contain a specific keyword in their website address, and the n_subpages shortest urls. n_subpages is an integer number and is one of the parameters that is determined by the user. For example, if "covid" is in the url_keywords list, the following url will be selected because it contains one of the keywords:'siemens.de/covid-19'.






