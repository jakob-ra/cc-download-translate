# CommonCrawl Downloader

## Purpose
[CommonCrawl](https://commoncrawl.org/the-data/get-started/) is a nonprofit organization that crawls the web and freely provides its archives and datasets to the public. The Common Crawl corpus contains petabytes of data collected since 2013. It contains monthly updates of raw web page data that are hosted as WARC files on Amazon Web Services' (AWS) S3 storage servers located in the US-East-1 (Northern Virginia) AWS Region.

This script was written with the purpose of downloading and processing the raw web page data for a user-provided list of domain names (e.g. apple.com, walmart.com, microsoft.com). We first get the byte range within the WARC file where a specific subpage is stored by querying the CommonCrawl Index via Athena. Using the byte range, the raw html of a webpage is downloaded and parsed into clear text. Then all passages around mentions of a user-provided list of keywords are extracted. The passages, along with information about their source are uploaded in batches of csv files to S3. The output files can then be downloaded or further processed. The output files have the following format:

![image](https://user-images.githubusercontent.com/49194118/199245335-a00f27ad-01e4-470b-8a06-4f06a8efd4cb.png)

By using Fargate spot instances the processing is cheap (in case a task is interrupted, it is just re-attempted). Also transfer speed is maximal because we access the CommonCrawl data from the same AWS region where it is hosted.

## How to run
### AWS Permissions & Authentication
To run the script, you first need to make an [AWS account](https://aws.amazon.com/). You then need to [create](https://us-east-1.console.aws.amazon.com/iamv2/home) an IAM-User in the US-East-1 region and add the following permissions:

- AmazonAthenaFullAccess
- AWSBatchFullAccess
- IAMFullAccess
- AmazonS3FullAccess

After you created the user and added all permission, click on > Security credentials > create access key > Download .csv file. Provide the path to the credential file that you just downloaded under config.yml > credentials_csv_filepath.

You also need to [create](https://us-east-1.console.aws.amazon.com/iamv2/home#/roles) a role and add the following permissions:
- AmazonS3FullAccess	
- CloudWatchFullAccess	
- AmazonAthenaFullAccess	
- AmazonElasticContainerRegistryPublicReadOnly

Once you created the role and added all permissions, click on the role and click the copy symbol next to the role's ARN:

![image](https://user-images.githubusercontent.com/49194118/199257495-1abe5be3-ed21-45c9-bdd3-9566a0169838.png)

Provide the role's ARN that you just copied under config.yml > credentials_csv_filepath.

### Input parameters
* n_subpages: number of shortest subpages that are selected for each website address in the output table. 
* url_keywords: list of keywords that if any of them exists in a url address, that url is selected in the output table. 
* crawl: this parameter determines the desired timeframe. CRAWL-NAME-YYYY-WW – The name of the crawl and year + week it was initiated. (... OR crawl = 'CC-MAIN-2020-24' OR crawl = 'CC-MAIN-2020-50')
All input files should be csv. 
*Warning:* urls should not contain "https" or "www." upfront.


The resulting table contains the address of all the historical subpages of the urls in the input file, for the selected timeframe.   
* select_subpages: The merged table created through the inner_join function is very large. Therefore, we only select the subpages that contain a specific keyword in their website address, and the n_subpages shortest urls. n_subpages is an integer number and is one of the parameters that is determined by the user. For example, if "covid" is in the url_keywords list, the following url will be selected because it contains one of the keywords:'siemens.de/covid-19'.






