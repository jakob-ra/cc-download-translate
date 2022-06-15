# first, create an IAM role with full access to S3, Athena, and EC2/Batch
# Then, configure the session with your AWS credentials by running "aws configure" in your terminal
import boto3
import athena_query

# params
s3path_url_list = 's3://cc-extract/dataprovider_all_months/' # folder where url list is stored, needs to be without 'www.'
output_bucket = 'cc-extract' # bucket to store the results
output_path = 'test' # path in output_bucket to store the results
params = {
    'region': 'us-east-1',
    'catalog': 'AwsDataCatalog',
    'database': 'ccindex',
    'bucket': output_bucket,
    'path': output_path
}

# start up session
session = boto3.Session()
client = session.client('athena')

def create_url_list_table(client, params):
    params['query'] = f"""CREATE EXTERNAL TABLE IF NOT EXISTS url_list (
    websiteaddress               STRING, 
    bvdidnumber                  STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    LOCATION '{s3path_url_list}'
    TBLPROPERTIES ('skip.header.line.count' = '1')
    ;"""
    athena_query.query_results(client, params)

def create_ccindex_table(client, params):
    params['query'] = f"""CREATE EXTERNAL TABLE IF NOT EXISTS ccindex (
    url_surtkey                   STRING,
    url                           STRING,
    url_host_name                 STRING,
    url_host_tld                  STRING,
    url_host_2nd_last_part        STRING,
    url_host_3rd_last_part        STRING,
    url_host_4th_last_part        STRING,
    url_host_5th_last_part        STRING,
    url_host_registry_suffix      STRING,
    url_host_registered_domain    STRING,
    url_host_private_suffix       STRING,
    url_host_private_domain       STRING,
    url_protocol                  STRING,
    url_port                      INT,
    url_path                      STRING,
    url_query                     STRING,
    fetch_time                    TIMESTAMP,
    fetch_status                  SMALLINT,
    content_digest                STRING,
    content_mime_type             STRING,
    content_mime_detected         STRING,
    content_charset               STRING,
    content_languages             STRING,
    warc_filename                 STRING,
    warc_record_offset            INT,
    warc_record_length            INT,
    warc_segment                  STRING)
    PARTITIONED BY (
    crawl                         STRING,
    subset                        STRING)
    STORED AS parquet
    LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';"""
    athena_query.query_results(client, params)

def repair_ccindex_table(client, params):
    params['query'] = f"""MSCK REPAIR TABLE ccindex;"""
    athena_query.query_results(client, params)

def inner_join(client, params):
    params['query'] = f"""CREATE TABLE urls_merged_cc AS
    SELECT url,
           url_host_name,
           url_host_registered_domain,
           warc_filename,
           warc_record_offset,
           warc_record_offset + warc_record_length - 1 as warc_record_end,
           crawl,
           subset
    FROM ccindex
    JOIN url_list ON ccindex.url_host_registered_domain = url_list.websiteaddress
    WHERE (crawl = 'CC-MAIN-2020-16')
            -- OR crawl = 'CC-MAIN-2020-24'
            -- OR crawl = 'CC-MAIN-2020-29' 
            -- OR crawl = 'CC-MAIN-2020-34'
            -- OR crawl = 'CC-MAIN-2020-40' 
            -- OR crawl = 'CC-MAIN-2020-45'
            -- OR crawl = 'CC-MAIN-2020-50'
            -- OR crawl = 'CC-MAIN-2021-04'
            -- OR crawl = 'CC-MAIN-2021-10'
            -- OR crawl = 'CC-MAIN-2021-17'
            -- OR crawl = 'CC-MAIN-2021-21'
            -- OR crawl = 'CC-MAIN-2021-25'
            -- OR crawl = 'CC-MAIN-2021-31'
            -- OR crawl = 'CC-MAIN-2021-39'
            -- OR crawl = 'CC-MAIN-2021-43'
            -- OR crawl = 'CC-MAIN-2021-49' 
            -- OR crawl = 'CC-MAIN-2022-05')
      AND subset = 'warc'"""
    athena_query.query_results(client, params)

create_url_list_table(client, params)
create_ccindex_table(client, params)
repair_ccindex_table(client, params)
inner_join(client, params)

params['query'] = 'SELECT * FROM ccindex limit 10;'

location, result = athena_query.query_results(client, params)




res = client.start_query_execution(QueryString=params['query'],
            QueryExecutionContext={'Database': params['database'], 'Catalog': params['catalog']},
            ResultConfiguration={'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']})

response = client.get_table_metadata(
    CatalogName=params['catalog'],
    DatabaseName=params['database'],
    TableName='ccindex'
)

results = client.get_query_results(QueryExecutionId=res)
