import time
import pandas as pd
import sys
import math

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

class Athena_lookup():
    def __init__(self, session, aws_params: dict, s3path_url_list, crawls: list, n_subpages: int, url_keywords: list,
                 athena_price_per_tb=5, wait_seconds=3600, limit_cc_table=10000, keep_ccindex=False):
        self.athena_client = session.client('athena')
        self.s3_client = session.client('s3')
        self.aws_params = aws_params
        self.s3path_url_list = s3path_url_list
        self.crawls = crawls
        self.n_subpages = n_subpages
        self.url_keywords = url_keywords
        self.athena_price_per_tb = athena_price_per_tb
        self.wait_seconds = wait_seconds
        self.limit_cc_table = limit_cc_table # to keep costs low for debugging, this should be None for full table
        self.total_cost = 0
        self.ccindex_table_name = 'ccindex'
        self.keep_ccindex = keep_ccindex


    @staticmethod
    def get_var_char_values(d):
        return [obj['VarCharValue'] if len(obj) > 0 else None for obj in d['Data']]

    def query_results(self, return_results=False):
        try:
            athena_client = self.athena_client
            params = self.aws_params
            wait_seconds = self.wait_seconds
            print('Starting query:\n' + params['query'])
            ## This function executes the query and returns the query execution ID
            response_query_execution_id = athena_client.start_query_execution(QueryString=params['query'],
                 QueryExecutionContext={'Database': params['database']},
                 ResultConfiguration={'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']}
                 )

            while wait_seconds > 0:
                wait_seconds = wait_seconds - 1
                response_get_query_details = athena_client.get_query_execution(
                        QueryExecutionId=response_query_execution_id['QueryExecutionId'])
                status = response_get_query_details['QueryExecution']['Status']['State']
                execution_time = response_get_query_details['QueryExecution']['Statistics'][
                    'TotalExecutionTimeInMillis']
                try:
                    data_scanned = response_get_query_details['QueryExecution']['Statistics'][
                        'DataScannedInBytes']
                except KeyError:
                    data_scanned = 0
                cost = data_scanned * 1e-12 * self.athena_price_per_tb


                if (status == 'FAILED') or (status == 'CANCELLED'):
                    failure_reason = response_get_query_details['QueryExecution']['Status'][
                        'StateChangeReason']
                    try:
                        self.total_cost += cost
                    except:
                        pass
                    print(failure_reason)
                    sys.exit(0)

                elif status == 'SUCCEEDED':
                    location = response_get_query_details['QueryExecution']['ResultConfiguration'][
                        'OutputLocation']
                    try:
                        self.total_cost += cost
                    except:
                        pass
                    print('')
                    # get size of output file
                    try:
                        output_size = self.s3_client.head_object(Bucket=params['bucket'],
                                            Key=location.split(params['bucket'] +'/')[1])['ContentLength']
                        print(f'Query successful! Results are available at {location}. Total size: {convert_size(output_size)}')
                    except:
                        print(f'Query successful! Results are available at {location}.')

                    ## Function to get output results
                    if return_results:
                        response_query_result = client.get_query_results(
                                QueryExecutionId=response_query_execution_id['QueryExecutionId'])
                        result_data = response_query_result['ResultSet']

                        if len(result_data['Rows']) > 1:
                            print('Results are available at ' + location)
                            if return_results:
                                header = result_data['Rows'][0]
                                rows = result_data['Rows'][1:]
                                header = get_var_char_values(header)
                                result = pd.DataFrame(
                                        [dict(zip(header, get_var_char_values(row))) for row in rows])
                                return location, result

                        else:
                            print('Result has zero length.')
                            return location, None

                    else:
                        return location, None

                else:
                    print(
                        f'Time elapsed: {execution_time / 1000}s. Data scanned: {convert_size(data_scanned)}. Total cost: {self.total_cost+cost:.2f}$.',
                        end='\r')
                    time.sleep(1)

            print('Timed out after 1 hour.')
            return None, None

        except KeyboardInterrupt:
            self.athena_client.stop_query_execution(QueryExecutionId=response_query_execution_id['QueryExecutionId'])
            print('Keyboard interrupt: Query cancelled.')
            sys.exit(0)

    def execute_query(self, query):
        self.aws_params['query'] = query
        self.query_results()

    def drop_all_tables(self):
        query = f"""DROP TABLE IF EXISTS url_list;"""
        self.execute_query(query)
        if not self.keep_ccindex:
            query = f"""DROP TABLE IF EXISTS ccindex;"""
            self.execute_query(query)
        query = f"""DROP TABLE IF EXISTS ccindex_smaller;"""
        self.execute_query(query)
        query = f"""DROP TABLE IF EXISTS urls_merged_cc;"""
        self.execute_query(query)

    def create_url_list_table(self):
        # to-do: make unspecific to bvdid
        query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS url_list (
        websiteaddress               STRING, 
        bvdidnumber                  STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        LOCATION '{self.s3path_url_list}'
        TBLPROPERTIES ('skip.header.line.count' = '1')
        ;"""
        self.execute_query(query)

    def create_ccindex_table(self):
        query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS ccindex (
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
        self.execute_query(query)

    def repair_ccindex_table(self):
        query = f"""MSCK REPAIR TABLE {self.ccindex_table_name};"""
        self.execute_query(query)

    # def subset_ccindex_table(self):
    #     # limit (for debugging)
    #     if self.limit_cc_table == None:
    #         limit = ''
    #     else:
    #         limit = 'LIMIT ' + str(self.limit_cc_table)
    #
    #     # if list of crawls specified, join them with OR: (crawl = 'CC-MAIN-2020-24' OR crawl = 'CC-MAIN-2020-52')
    #     crawls = '(' + ' OR '.join(f'crawl = \'{w}\'' for w in self.crawls) + ')'
    #
    #     query = f"""Create table ccindex_smaller AS
    #     SELECT url,
    #            url_host_name,
    #            url_host_registered_domain,
    #            warc_filename,
    #            warc_record_offset,
    #            warc_record_offset + warc_record_length - 1 as warc_record_end,
    #            crawl,
    #            subset
    #     FROM ccindex.ccindex
    #     WHERE {crawls}
    #       AND subset = 'warc'
    #     {limit};"""
    #     self.execute_query(query)
    #     self.ccindex_table_name = 'ccindex_smaller'

    # def inner_join(self):
    #     query = f"""CREATE TABLE urls_merged_cc AS
    #     SELECT *
    #     FROM {self.ccindex_table_name}, url_list
    #     WHERE {self.ccindex_table_name}.url_host_registered_domain = url_list.websiteaddress
    #     """
    #     self.execute_query(query)

    def inner_join(self):
        # limit (for debugging)
        if self.limit_cc_table:
            limit = 'LIMIT ' + str(self.limit_cc_table)
        else:
            limit = ''

        # if list of crawls specified, join them with OR: (crawl = 'CC-MAIN-2020-24' OR crawl = 'CC-MAIN-2020-52')
        crawls = '(' + ' OR '.join(f'crawl = \'{w}\'' for w in self.crawls) + ')'

        query = f"""CREATE TABLE urls_merged_cc AS
        SELECT url,
               url_host_name,
               url_host_registered_domain,
               warc_filename,
               warc_record_offset,
               warc_record_offset + warc_record_length - 1 as warc_record_end,
               crawl,
               subset
        FROM {self.ccindex_table_name}, url_list
        WHERE {crawls}
          AND subset = 'warc'
          AND {self.ccindex_table_name}.url_host_registered_domain = url_list.websiteaddress
        {limit}"""
        self.execute_query(query)

    def select_subpages(self):
        query = f"""select url,
                    url_host_name,
                    url_host_registered_domain,
                    warc_filename,
                    warc_record_offset,
                    warc_record_end,
                    crawl
                    from (
                select url,
                    url_host_name,
                    url_host_registered_domain,
                    warc_filename,
                    warc_record_offset,
                    warc_record_end,
                    crawl,
                    row_number() over (partition by url_host_name order by length(url) asc) as subpage_rank 
                from urls_merged_cc) ranks
            where subpage_rank <= {self.n_subpages}
            
            UNION
            
            SELECT url,
                    url_host_name,
                    url_host_registered_domain,
                    warc_filename,
                    warc_record_offset,
                    warc_record_end,
                    crawl
                    FROM urls_merged_cc
            WHERE """ + ' OR '.join([f"url LIKE '%{keyword}%'" for keyword in self.url_keywords])

        self.execute_query(query)

    def run_lookup(self):
        self.drop_all_tables()
        self.create_url_list_table()
        if not self.keep_ccindex:
            self.create_ccindex_table()
            self.repair_ccindex_table()
        # self.subset_ccindex_table()
        self.inner_join()
        self.select_subpages()




# awsparams['query'] = 'SELECT * FROM ccindex limit 10;'
#
# location, result = athena_query.query_results(client, aws_params)
#
#
#
# res = client.start_query_execution(QueryString=awsparams['query'],
#                                    QueryExecutionContext={'Database': awsparams['database'], 'Catalog': awsparams['catalog']},
#                                    ResultConfiguration={'OutputLocation': 's3://' + awsparams['bucket'] + '/' + awsparams['path']})
#
# response = client.get_table_metadata(
#     CatalogName=awsparams['catalog'],
#     DatabaseName=awsparams['database'],
#     TableName='ccindex'
# )
#
# results = client.get_query_results(QueryExecutionId=res)
