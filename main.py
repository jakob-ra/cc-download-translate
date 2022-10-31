import boto3
from athena_lookup import Athena_lookup
import pandas as pd
import os
import s3fs
from aws_config import aws_config_credentials
import time

# first, create IAM user with full access to S3, Athena, Batch, download access key file as csv
# change ecsTaskExecutionRole to have S3 access cloudwatch
# also create a role that can be passed to

region = 'us-east-1'
profile_name = 'default'
credentials_csv_filepath = 'C:/Users/Jakob/Downloads/jakob-s3-ec2-athena_accessKeys.csv'
aws_config_credentials(credentials_csv_filepath, region, profile_name)

# params
s3path_url_list = 's3://cc-extract/dataprovider_all_months' # folder where url list is stored, starts with 's3://'
output_bucket = 'cc-extract' # bucket to store the results
index_output_path = 'urls_merged_cc' # path in output_bucket to store the index results
crawls = ['CC-MAIN-2020-29', 'CC-MAIN-2020-34'] # crawl name, for list see https://commoncrawl.org/the-data/get-started/
# available_crawls = pd.read_csv('common-crawls.txt')
available_crawls = ['CC-MAIN-2020-16', 'CC-MAIN-2020-24', 'CC-MAIN-2020-29', 'CC-MAIN-2020-34',
                    'CC-MAIN-2020-40', 'CC-MAIN-2020-45', 'CC-MAIN-2020-50', 'CC-MAIN-2021-04',
                    'CC-MAIN-2021-10', 'CC-MAIN-2021-17', 'CC-MAIN-2021-21', 'CC-MAIN-2021-25',
                    'CC-MAIN-2021-31', 'CC-MAIN-2021-39', 'CC-MAIN-2021-43', 'CC-MAIN-2021-49',
                    'CC-MAIN-2022-05', 'CC-MAIN-2022-21', 'CC-MAIN-2022-27', 'CC-MAIN-2022-33']
output_path = 'cc-download/' + '_'.join(crawls) # path in output_bucket to store the downloads in batches

aws_params = {
    'region': region,
    'catalog': 'AwsDataCatalog',
    'database': 'ccindex',
    'bucket': output_bucket,
    'path': index_output_path
}

n_subpages = 10 # number of subpages to download per domain
url_keywords = ['covid', 'corona', 'coronavirus', 'news', 'press', 'update'] # additionaly include subpages with these keywords.csv in the url
news_translations = ["الإخبارية", "nieuws", "notizia", "ニュース", "nachrichten", "noticias", "nouvelles", "Новости"]
url_keywords += news_translations

keywords_path = 'https://github.com/jakob-ra/cc-download/raw/main/cc-download/keywords.csv'

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

batch_env_name = 'cc'

# # compute environment
# batch_client.create_compute_environment(
#     computeEnvironmentName=batch_env_name,
#     type='MANAGED',
#     state='ENABLED',
#     # unmanagedvCpus=1,
#     computeResources={
#         'type': 'SPOT', # 'EC2'|'SPOT'|'FARGATE'|'FARGATE_SPOT'
#         'allocationStrategy': 'SPOT_CAPACITY_OPTIMIZED', # |'BEST_FIT_PROGRESSIVE'|'SPOT_CAPACITY_OPTIMIZED'
#         'minvCpus': 1,
#         'maxvCpus': req_batches,
#         'desiredvCpus': req_batches,
#         'instanceTypes': [
#             'a1.medium'
#         ],
#         'instanceRole': 'arn:aws:iam::425352751544:instance-profile/ecsInstanceRole',
#         'subnets': [
#             'subnet-3fc5e11e',
#             'subnet-96402da7',
#             'subnet-84326be2',
#             'subnet-3470633a',
#             'subnet-789d7434',
#             'subnet-d4104a8b',
#         ],
#         'securityGroupIds': [
#             'sg-c4a092d8',
#         ],
#         # 'bidPercentage': 70,
#         # 'spotIamFleetRole': 'string',
#     },
#     # serviceRole = 'arn:aws:iam::425352751544:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch',
#     # serviceRole='arn:aws:iam::425352751544:user/jakob-s3-ec2-athena',
# )
# time.sleep(20)

# FARGATE
batch_client.create_compute_environment(
    computeEnvironmentName=batch_env_name,
    type='MANAGED',
    state='ENABLED',
    computeResources={
        'type': 'FARGATE_SPOT',
        'maxvCpus': req_batches,
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
    },
    tags={
        'Project': 'cc-download'
    },
    # serviceRole='arn:aws:iam::425352751544:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch',
)

# job queue
batch_client.create_job_queue(
    jobQueueName=batch_env_name,
    state='ENABLED',
    priority=1,
    computeEnvironmentOrder=[
        {
            'order': 1,
            'computeEnvironment': batch_env_name,
        },
    ],
)
time.sleep(5)

# job definition
# batch_client.register_job_definition(
#     jobDefinitionName=batch_env_name,
#     type='container',
#     containerProperties={
#         'image': 'public.ecr.aws/r9v1u7o6/cc-download:latest',
#         'resourceRequirements': [
#             {
#                 'type': 'VCPU',
#                 'value': '1',
#             },
#             {
#                 'type': 'MEMORY',
#                 'value': '2048',
#             },
#         ],
#         'command': [
#             "python",
#             "./cc-download/cc-download.py",
#             f"--batch_size {batch_size}",
#             f"--output_bucket {output_bucket}",
#             f"--output_path {output_path}",
#             f"--keywords {keywords_path}",
#         ],
#         'jobRoleArn': 'arn:aws:iam::425352751544:role/ecsTaskExecutionRole',
#     },
#     retryStrategy={
#         'attempts': 1,
#     },
#     timeout={
#         'attemptDurationSeconds': 600
#     },
#     platformCapabilities=[
#         'EC2',
#     ],
# )

# FARGATE
batch_client.register_job_definition(
    jobDefinitionName=batch_env_name,
    type='container',
    containerProperties={
        'image': 'public.ecr.aws/r9v1u7o6/cc-download:latest',
        'resourceRequirements': [
            {
                'type': 'VCPU',
                'value': '0.25',
            },
            {
                'type': 'MEMORY',
                'value': '512',
            },
        ],
        'command': [
            "python3",
            "./cc-download/cc-download.py",
            f"--batch_size={batch_size}",
            f"--output_bucket={output_bucket}",
            f"--output_path={output_path}",
            f"--keywords_path={keywords_path}",
        ],
        'jobRoleArn': 'arn:aws:iam::425352751544:role/cc-download', #'arn:aws:iam::425352751544:role/ecsTaskExecutionRole',
        'executionRoleArn':  'arn:aws:iam::425352751544:role/cc-download', # 'arn:aws:iam::425352751544:role/ecsTaskExecutionRole',
        'networkConfiguration': {
                'assignPublicIp': 'ENABLED',
        }
    },
    retryStrategy={
        'attempts': 3,
    },
    timeout={
        'attemptDurationSeconds': 1800
    },
    platformCapabilities=[
        'FARGATE',
    ],
)
time.sleep(5)

# submit job
batch_client.submit_job(
    jobName=batch_env_name,
    jobQueue=batch_env_name,
    arrayProperties={
        'size': req_batches,
    },
    jobDefinition=batch_env_name,
    # retryStrategy={
    #     'attempts': 2,
    # },
    # timeout={
    #     'attemptDurationSeconds': 1800
    # },
)




# estimate cost
compute_time = req_batches * 600 / 3600
0.01293507*compute_time*0.25 + 0.00142037*compute_time*0.5 # FARGATE SPOT
0.0004*req_batches*batch_size/1000 # S3 requests
0.37*len(crawls) # athena lookup cost
# athena retrieval costs

# instance_price_per_hour = 0.0035
# time_for_completion = 0.1 # 6 minutes
# instance_price_per_hour*req_batches*time_for_completion*len(available_crawls)


res = pd.read_csv(athena_lookup.download_table_location)

df = pd.read_csv(athena_lookup.download_table_location, skiprows=range(1, 17000 * batch_size), nrows=batch_size, header=0)

pd.read_csv(keywords_path).squeeze().tolist()


# get rid of old compute environment
try:
    # batch_client.deregister_job_definition(jobDefinition=batch_env_name)
    batch_client.update_job_queue(jobQueue=batch_env_name, state='DISABLED')
    time.sleep(2)
    batch_client.delete_job_queue(jobQueue=batch_env_name)
    time.sleep(2)
    batch_client.update_compute_environment(
        computeEnvironment=batch_env_name,
        state='DISABLED',
    )
    time.sleep(2)
    batch_client.delete_compute_environment(
        computeEnvironment='cc-download'
    )
    time.sleep(2)
except Exception as e:
    print(e)




# streamlined:
# keywords.csv = ["covid", "SARS‑CoV‑2", "pandemic", "corona", "SARS", "pandemie", "pandémie", "pandemia",
#             "pandemi", "Korona", "Wuhan-Virus", "cobiçado-19", "كورونا", "جائحة", "电晕大流行", "电晕", "冠状病毒",
#             "冠状流感", "日冕", "武汉病毒", "大流行", "大流行", "2019年nCoV", "大流行", "कोरोना", "कोरोना", "महामारी",
#             "コロナパンデミック", "コロナ", "コロナウイルス", "コロナパンデミ", "コロナクリス", "武漢ウイルス", "パンデミー", "パンデミック", "パンデミー",
#             "パンデミック", "الاكليل", "التاج", "السارس", "بانديمي", "جائحة", "пандемия", "корона", "Ковид"]

# old version:
# keywords = ["covid", "SARS‑CoV‑2", "corona pandemic", "corona", "covid 19" , "covid-19"
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
#             "パンデミック", "パンデミー", "パンデミック", "الاكليل", "جائحة الاكليل",
#             "مطمع ۱۹", "الفيروس التاجي", "التاج", "السارس", "فيروس ووهان",
#             "بانديمي", "جائحة", "ncov 2019", "2019 ncov", "пандемия короны",
#             "корона", "Ковид 19", "коронавирус", "ш и ndemie", "о в nakrise",
#             "Уханьский вирус", "ndemie", "пандемия", "2019 нКоВ", "ndemia"]
# pd.Series(keywords, name='keyword').to_csv('keywords.csv', index=False)

df = pd.concat([pd.read_csv(f's3://{output_bucket}/{output_path}/batch_n_{i}.csv') for i in range(req_batches)])

df = pd.concat([pd.read_csv(f's3://{output_bucket}/cc-download/CC-MAIN-2020-24/batch_n_{i}.csv') for i in range(req_batches//100)])

# df = pd.read_csv(f's3://{output_bucket}/cc-download/CC-MAIN-2020-16/batch_n_{0}.csv')

from ast import literal_eval
df['paragraphs'] = df.paragraphs.apply(literal_eval)
df = df.explode('paragraphs')
df['paragraphs'] = df.paragraphs.str.strip()
df.drop(columns=['warc_filename', 'warc_record_offset', 'warc_record_end'], inplace=True)

from langdetect import detect
def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except:
        return None

df['lang'] = df.paragraphs.str[:50].apply(detect_lang)

lang_counts = df.lang.value_counts().to_frame(name='count')
lang_counts['share'] = lang_counts/lang_counts.sum()
lang_counts['cum_share'] = lang_counts.share.cumsum()

lang_codes = pd.read_csv('/Users/Jakob/Downloads/language-codes.csv', names=['lang', 'language'])
lang_counts = lang_counts.reset_index(names='lang').merge(lang_codes, on='lang')

import matplotlib.pyplot as plt
plt.figure(figsize=(10,4))
lang_counts.cum_share.plot(marker='o')
plt.xticks(range(len(lang_counts.index)), lang_counts.language, rotation=45)
plt.grid()
plt.show()

lang_counts.head(12) # take top 12 languages, 95% of content


import urllib.request
import argostranslate.package
import argostranslate.translate


def download_install_argos_model(from_code, to_code):
    argos_models = pd.read_json('https://github.com/argosopentech/argospm-index/raw/main/index.json')
    argos_link = argos_models[(argos_models.from_code == from_code) & (argos_models.to_code == to_code)].iloc[0].links[0]
    argos_model_name = argos_link.split('/')[-1]
    urllib.request.urlretrieve(argos_link, argos_model_name)
    argostranslate.package.install_from_path(argos_model_name)

def load_argos_model(from_code, to_code):
    installed_languages = argostranslate.translate.get_installed_languages()
    from_lang = list(filter(lambda x: x.code == from_code, installed_languages))[0]
    to_lang = list(filter(lambda x: x.code == to_code, installed_languages))[0]
    model = from_lang.get_translation(to_lang)

    return model

for lang in ['de', 'es', 'nl', 'fr', 'pt', 'it', 'ja', 'ru', 'id', 'sv', 'pl']:
    from_code = lang
    to_code = "en"
    download_install_argos_model(from_code, to_code)
    
model = load_argos_model(from_code, to_code)
translatedText = model.translate("Hallo, wie geht es Ihnen heute?")
print(translatedText)


# Download and install Argos Translate package
available_packages = argostranslate.package.get_available_packages()
package_to_install = list(filter(lambda x: x.from_code == from_code and x.to_code == to_code, available_packages))[0]
argostranslate.package.install_from_path(package_to_install.download())

# Translate
translatedText = argostranslate.translate.translate("Hello World", from_code, to_code)
print(translatedText)

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
#         description='Download html paragraphs containing keywords.csv from CommonCrawl archive, using an URL list.',
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
# keywords.csv = ["covid", "SARS‑CoV‑2", "corona pandemic", "corona",  "covid 19" , "covid-19"
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
#                                  any(ext.casefold() in paragraph.casefold() for ext in keywords.csv)]
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