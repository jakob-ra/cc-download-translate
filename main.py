import boto3
from athena_lookup import Athena_lookup
import pandas as pd
import os
import s3fs
from aws_config import aws_config_credentials
import time

region = 'us-east-1'
profile_name = 'default'
credentials_csv_filepath = 'C:/Users/Jakob/Downloads/jakob-s3-ec2-athena_accessKeys.csv'
aws_config_credentials(credentials_csv_filepath, region, profile_name)

# params
s3path_url_list = 's3://cc-extract/dataprovider_all_months' # folder where url list is stored, starts with 's3://'
output_bucket = 'cc-extract' # bucket to store the results
output_path = 'urls_merged_cc' # path in output_bucket to store the results
crawls = ['CC-MAIN-2020-24'] # crawl name, for list see https://commoncrawl.org/the-data/get-started/
# available_crawls = pd.read_csv('common-crawls.txt')
available_crawls = ['CC-MAIN-2020-16', 'CC-MAIN-2020-24', 'CC-MAIN-2020-29', 'CC-MAIN-2020-34',
                    'CC-MAIN-2020-40', 'CC-MAIN-2020-45', 'CC-MAIN-2020-50', 'CC-MAIN-2021-04',
                    'CC-MAIN-2021-10', 'CC-MAIN-2021-17', 'CC-MAIN-2021-21', 'CC-MAIN-2021-25',
                    'CC-MAIN-2021-31', 'CC-MAIN-2021-39', 'CC-MAIN-2021-43', 'CC-MAIN-2021-49',
                    'CC-MAIN-2022-05', 'CC-MAIN-2022-21', 'CC-MAIN-2022-27', 'CC-MAIN-2022-33']

aws_params = {
    'region': region,
    'catalog': 'AwsDataCatalog',
    'database': 'ccindex',
    'bucket': output_bucket,
    'path': output_path
}

n_subpages = 10 # number of subpages to download per domain
url_keywords = ['covid', 'corona', 'news', 'press', 'update'] # additionaly include subpages with these keywords in the url

# start up session
session = boto3.Session()

athena_lookup = Athena_lookup(session, aws_params, s3path_url_list, crawls, n_subpages, url_keywords,
                              limit_cc_table=None, keep_ccindex=True)
# athena_lookup.drop_all_tables()
# athena_lookup.create_url_list_table()
# # athena_lookup.create_ccindex_table()
# # athena_lookup.repair_ccindex_table()
# athena_lookup.subset_ccindex_table()
# athena_lookup.inner_join()
# athena_lookup.select_subpages()
athena_lookup.run_lookup()

batch_size = 5000
req_batches = int(athena_lookup.download_table_length//batch_size + 1)
print(f'Splitting {athena_lookup.download_table_length} subpages into {req_batches} batches of size {batch_size}.')

## send commands for AWS batch download
batch_client = session.client('batch')

# get rid of old
try:
    batch_client.update_compute_environment(
        computeEnvironment='cc-download',
        state='DISABLED',
    )
    time.sleep(20)
    batch_client.delete_compute_environment(
        computeEnvironment='cc-download'
    )
    time.sleep(30)
except:
    pass

attempt_n = 6

# compute environment
batch_client.create_compute_environment(
    computeEnvironmentName=f'cc-download-{attempt_n}',
    type='MANAGED',
    state='ENABLED',
    # unmanagedvCpus=1,
    computeResources={
        'type': 'SPOT', # 'EC2'|'SPOT'|'FARGATE'|'FARGATE_SPOT'
        'allocationStrategy': 'SPOT_CAPACITY_OPTIMIZED', # |'BEST_FIT_PROGRESSIVE'|'SPOT_CAPACITY_OPTIMIZED'
        'minvCpus': 1,
        'maxvCpus': req_batches,
        'desiredvCpus': req_batches,
        'instanceTypes': [
            'optimal',
        ],
        'instanceRole': 'arn:aws:iam::425352751544:instance-profile/ecsInstanceRole',
        'subnets': [
            'subnet-3fc5e11e',
            'subnet-96402da7',
            'subnet-84326be2',
            'subnet-3470633a',
            'subnet-789d7434',
            'subnet-d4104a8b',
        ],
        'securityGroupIds': [
            'sg-c4a092d8',
        ],
        # 'bidPercentage': 70,
        # 'spotIamFleetRole': 'string',
    },
    serviceRole = 'arn:aws:iam::425352751544:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch',
    # serviceRole='arn:aws:iam::425352751544:user/jakob-s3-ec2-athena',
)
time.sleep(20)

# job queue
batch_client.create_job_queue(
    jobQueueName=f'cc-download-{attempt_n}',
    state='ENABLED',
    priority=1,
    computeEnvironmentOrder=[
        {
            'order': 1,
            'computeEnvironment': f'cc-download-{attempt_n}'
        },
    ],
)
time.sleep(5)

# job definition
batch_client.register_job_definition(
    jobDefinitionName=f'cc-download-{attempt_n}',
    type='container',
    containerProperties={
        'image': 'public.ecr.aws/r9v1u7o6/cc-download:latest',
        'vcpus': 1,
        'memory': 2048,
        'command': [
            "python",
            "./cc-download/cc-download.py",
            f"--batch_size={batch_size}",
        ],
        'jobRoleArn': 'arn:aws:iam::425352751544:role/ecsTaskExecutionRole',
        'executionRoleArn': 'arn:aws:iam::425352751544:role/ecsTaskExecutionRole',
    },
    retryStrategy={
        'attempts': 1,
    },
    timeout={
        'attemptDurationSeconds': 1800
    },
    platformCapabilities=[
        'EC2',
    ],
)
time.sleep(5)

# submit job
batch_client.submit_job(
    jobName=f'cc-download-{attempt_n}',
    jobQueue=f'cc-download-{attempt_n}',
    arrayProperties={
        'size': req_batches
    },
    jobDefinition=f'cc-download-{attempt_n}',
    retryStrategy={
        'attempts': 1,
    },
    timeout={
        'attemptDurationSeconds': 1800
    },
)






# estimate cost
instance_price_per_hour = 0.0035
time_for_completion = 0.1 # 6 minutes
instance_price_per_hour*req_batches*time_for_completion*len(available_crawls)


res = pd.read_csv(athena_lookup.download_table_location)

df = pd.read_csv(athena_lookup.download_table_location, skiprows=range(1, 17000 * batch_size), nrows=batch_size, header=0)



















# df = pd.read_csv('/Users/Jakob/Downloads/61df3a7f-7b61-474c-ae91-1a6dd08d2ded.csv')
#
# df.url_host_registered_domain.value_counts().head(10000).sum()
#
#
# df.groupby('url_host_registered_domain').head(100)
#
# subpages_per_job = 1000 # number of subpages to download per job


# def main():
#     parser = argparse.ArgumentParser(
#         prog='athena',
#         usage='athena [--debug] [--execute <statement>] [--output-format <format>] [--schema <schema>]'
#               ' [--profile <profile>] [--region <region>] [--s3-bucket <bucket>] [--server-side-encryption] [--version]',
#         description='Download html paragraphs containing keywords from CommonCrawl archive, using an URL list.',
#     )
#     parser.add_argument(
#         '--debug',
#         action='store_true',
#         help='enable debug mode'
#     )
#
# if __name__ == '__main__':
#     main()



# import pandas as pd
# import os
# import numpy as np
# import boto3
# from warcio.archiveiterator import ArchiveIterator
# import swifter
# import timeit
# import time
# from io import BytesIO
# from bs4 import BeautifulSoup, SoupStrainer
# import lxml
# import cchardet
# import botocore
# import dask.dataframe as dd
#
#
# covid_synonyms = ["covid", "SARS‑CoV‑2", "corona pandemic", "corona",  "covid 19" , "covid-19"
#                   , "corona virus", "coronapandemie", "coronakrise", "SARS CoV 2",
#                   "Wuhan virus", "pandemie", "pandemic", "2019 nCoV", "pandémie",
#                   "pandemia", "Koronapandemie", "Korona", "Coronavirus",
#                   "Coronapandemie", "Wuhan-Virus", "pandémie corona", "Virus de Wuhan", "NCoV 2019",
#                   "pandemia de corona", "coronavirus", "coronapandemia", "cobiçado 19"
#                   , "coronavírus", "Vírus Wuhan", "电晕大流行", "电晕", "冠状病毒", "冠状流感",
#                   "日冕", "武汉病毒", "大流行", "大流行", "2019年nCoV",
#                   "大流行", "कोरोना महामारी" , "कोरोना", "कोविद १ ९", "कोरोना", "ओ और ndemie",
#                   "ओ nakrise", "वुहान वायरस", "ndemie", "महामारी", "コロナパンデミック", "コロナ",
#                   "コロナウイルス", "コロナパンデミ", "コロナクリス", "武漢ウイルス", "パンデミー",
#                   "パンデミック", "パンデミー", "パンデミック", "الاكليل", "جائحة الاكليل",
#                   "مطمع ۱۹", "الفيروس التاجي", "التاج", "السارس", "فيروس ووهان",
#                   "بانديمي", "جائحة", "ncov 2019", "2019 ncov", "пандемия короны",
#                   "корона", "Ковид 19", "коронавирус", "ш и ndemie", "о в nakrise",
#                   "Уханьский вирус", "ndemie", "пандемия", "2019 нКоВ", "ndemia"]
#
#
# df_sample = pd.read_csv('s3://cc-extract/top10subpages/2022/05/31/8f1d841c-8c64-4c84-85f8-e73c352e156f.csv',
#                  nrows=1000)
#
#
# def fetch_process_warc_records(batch):
#     """Fetch all WARC records defined by filenames and offsets in batch,
#     parse the records and the contained HTML, split the text into words
#     and emit pairs <word, 1>"""
#     batch['input'] = batch.url + '<;>' + batch.warc_filename + '<;>' + batch.warc_record_offset.apply(
#         str) + '<;>' + batch.warc_record_end.apply(str)
#     batch = batch.input.values
#
#     s3client = boto3.client('s3', region_name='us-east-1', use_ssl=False)
#
#     only_paragraphs = SoupStrainer('p')
#
#     results = []
#     for x in batch:
#         url, warc_path, offset, end = x.split('<;>')
#         rangereq = 'bytes={}-{}'.format(offset, end)
#
#         response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)
#
#         record_stream = BytesIO(response["Body"].read())
#
#         covid_paragraphs = []
#         for record in ArchiveIterator(record_stream):
#             page = record.content_stream().read()
#
#             soup = BeautifulSoup(page, 'lxml', parse_only=only_paragraphs)
#             text = soup.get_text()
#
#             paragraphs = text.split('\n')
#             covid_paragraphs += [paragraph for paragraph in paragraphs if
#                                  any(ext.casefold() in paragraph.casefold() for ext in covid_synonyms)]
#
#         res = '\n'.join(covid_paragraphs)
#         results.append(res)
#
#     return results
#
#
# start = time.process_time()
# ddf = dd.from_pandas(df, npartitions=os.cpu_count()-2)
#
# result = ddf.map_partitions(fetch_process_warc_records, meta=list)
# result = result.compute(num_workers=os.cpu_count()-2)
# print(time.process_time() - start)

# # to find required number of batches
# import awswrangler as wr
# df = wr.athena.read_sql_query(sql="SELECT COUNT(*) AS count FROM urls_merged_cc_for_download", database="ccindex")
# row_count = df.T.squeeze()
# batch_size = 1000
# req_batches = row_count//batch_size + 1