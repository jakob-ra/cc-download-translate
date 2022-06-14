import pandas as pd
import os
import numpy as np
import boto3
from warcio.archiveiterator import ArchiveIterator
import swifter
import timeit
import time
from io import BytesIO
from bs4 import BeautifulSoup, SoupStrainer
import lxml
import cchardet
import botocore
import dask.dataframe as dd


covid_synonyms = ["covid", "SARS‑CoV‑2", "corona pandemic", "corona",  "covid 19" , "covid-19"
                  , "corona virus", "coronapandemie", "coronakrise", "SARS CoV 2",
                  "Wuhan virus", "pandemie", "pandemic", "2019 nCoV", "pandémie",
                  "pandemia", "Koronapandemie", "Korona", "Coronavirus",
                  "Coronapandemie", "Wuhan-Virus", "pandémie corona", "Virus de Wuhan", "NCoV 2019",
                  "pandemia de corona", "coronavirus", "coronapandemia", "cobiçado 19"
                  , "coronavírus", "Vírus Wuhan", "电晕大流行", "电晕", "冠状病毒", "冠状流感",
                  "日冕", "武汉病毒", "大流行", "大流行", "2019年nCoV",
                  "大流行", "कोरोना महामारी" , "कोरोना", "कोविद १ ९", "कोरोना", "ओ और ndemie",
                  "ओ nakrise", "वुहान वायरस", "ndemie", "महामारी", "コロナパンデミック", "コロナ",
                  "コロナウイルス", "コロナパンデミ", "コロナクリス", "武漢ウイルス", "パンデミー",
                  "パンデミック", "パンデミー", "パンデミック", "الاكليل", "جائحة الاكليل",
                  "مطمع ۱۹", "الفيروس التاجي", "التاج", "السارس", "فيروس ووهان",
                  "بانديمي", "جائحة", "ncov 2019", "2019 ncov", "пандемия короны",
                  "корона", "Ковид 19", "коронавирус", "ш и ndemie", "о в nakrise",
                  "Уханьский вирус", "ndemie", "пандемия", "2019 нКоВ", "ndemia"]


df_sample = pd.read_csv('s3://cc-extract/top10subpages/2022/05/31/8f1d841c-8c64-4c84-85f8-e73c352e156f.csv',
                 nrows=1000)


def fetch_process_warc_records(batch):
    """Fetch all WARC records defined by filenames and offsets in batch,
    parse the records and the contained HTML, split the text into words
    and emit pairs <word, 1>"""
    batch['input'] = batch.url + '<;>' + batch.warc_filename + '<;>' + batch.warc_record_offset.apply(
        str) + '<;>' + batch.warc_record_end.apply(str)
    batch = batch.input.values

    s3client = boto3.client('s3', region_name='us-east-1', use_ssl=False)

    only_paragraphs = SoupStrainer('p')

    results = []
    for x in batch:
        url, warc_path, offset, end = x.split('<;>')
        rangereq = 'bytes={}-{}'.format(offset, end)

        response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)

        record_stream = BytesIO(response["Body"].read())

        covid_paragraphs = []
        for record in ArchiveIterator(record_stream):
            page = record.content_stream().read()

            soup = BeautifulSoup(page, 'lxml', parse_only=only_paragraphs)
            text = soup.get_text()

            paragraphs = text.split('\n')
            covid_paragraphs += [paragraph for paragraph in paragraphs if
                                 any(ext.casefold() in paragraph.casefold() for ext in covid_synonyms)]

        res = '\n'.join(covid_paragraphs)
        results.append(res)

    return results


start = time.process_time()
ddf = dd.from_pandas(df, npartitions=os.cpu_count()-2)

result = ddf.map_partitions(fetch_process_warc_records, meta=list)
result = result.compute(num_workers=os.cpu_count()-2)
print(time.process_time() - start)