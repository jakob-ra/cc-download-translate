# first, create an IAM role with full access to S3, Athena, and EC2/Batch
# Then, configure the session with your AWS credentials by running "aws configure" in your terminal
import boto3
from athena_lookup import Athena_lookup
import argparse
import pandas as pd

# params
s3path_url_list = 's3://cc-extract/dataprovider_all_months_sample' # folder where url list is stored, needs to be without 'www.'
output_bucket = 'cc-extract' # bucket to store the results
output_path = 'urls_merged_cc' # path in output_bucket to store the results
crawl='CC-MAIN-2020-16' # crawl name, for list see https://commoncrawl.org/the-data/get-started/
available_crawls = ['CC-MAIN-2020-16', 'CC-MAIN-2020-24', 'CC-MAIN-2020-29', 'CC-MAIN-2020-34',
                    'CC-MAIN-2020-40', 'CC-MAIN-2020-45', 'CC-MAIN-2020-50', 'CC-MAIN-2021-04',
                    'CC-MAIN-2021-10', 'CC-MAIN-2021-17', 'CC-MAIN-2021-21', 'CC-MAIN-2021-25',
                    'CC-MAIN-2021-31', 'CC-MAIN-2021-39', 'CC-MAIN-2021-43', 'CC-MAIN-2021-49',
                    'CC-MAIN-2022-05']

aws_params = {
    'region': 'us-east-1',
    'catalog': 'AwsDataCatalog',
    'database': 'ccindex',
    'bucket': output_bucket,
    'path': output_path
}
n_subpages = 10 # number of subpages to download per domain
url_keywords = ['covid', 'corona', 'news', 'press', 'update'] # additionaly include subpages with these keywords in the url

# start up session
session = boto3.Session()

athena_lookup = Athena_lookup(session, aws_params, s3path_url_list, crawl, n_subpages, url_keywords)
# athena_lookup.drop_all_tables()
# athena_lookup.create_url_list_table()
# athena_lookup.create_ccindex_table()
# athena_lookup.repair_ccindex_table()
# athena_lookup.inner_join()
# athena_lookup.select_subpages()
athena_lookup.run_lookup()

df = pd.read_csv('/Users/Jakob/Downloads/61df3a7f-7b61-474c-ae91-1a6dd08d2ded.csv')

df.url_host_registered_domain.value_counts().head(10000).sum()


df.groupby('url_host_registered_domain').head(100)

subpages_per_job = 1000 # number of subpages to download per job


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