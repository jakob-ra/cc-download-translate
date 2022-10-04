import pandas as pd
import boto3
from warcio.archiveiterator import ArchiveIterator
import time
from io import BytesIO
from bs4 import BeautifulSoup, SoupStrainer
import os
import argparse

# # parse arguments: # python cc-download.py --batch_size=100 --batch_number=5
parser = argparse.ArgumentParser()
parser.add_argument("--batch_size", type=int, default=100)
parser.add_argument("--output_bucket", type=str, default='cc-extract')
parser.add_argument("--output_path", type=str, default='cc-download')
args = parser.parse_args()



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


def fetch_process_warc_records(row, s3client):
    """Fetch all WARC records defined by filenames and offsets in batch,
    parse the records and the contained HTML, return all paragraphs containing at least one of the
    keywords"""

    only_paragraphs = SoupStrainer('p')

    warc_path, offset, end = row.warc_filename, str(row.warc_record_offset), str(row.warc_record_end)

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

    return list(set(covid_paragraphs))

if __name__ == "__main__":
    if "AWS_BATCH_JOB_ARRAY_INDEX" in os.environ:
        batch_n = os.environ['AWS_BATCH_JOB_ARRAY_INDEX']
        batch_n = int(batch_n)
        print(f'Processing batch {batch_n}.')
    else:
        batch_n = 0
        print('Processing first batch (no array index found).')


    # read cc-index table with warc filenames and byte positions
    df = pd.read_csv('s3://cc-extract/urls_merged_cc/fc16332b-b3a1-449c-b440-ab0d060fdd08.csv',
                     skiprows=range(1,batch_n*args.batch_size), nrows=args.batch_size, header=0)

    # initialize s3
    s3client = boto3.client('s3', region_name='us-east-1', use_ssl=False)

    # download paragraphs and fill into new column
    start = time.process_time()
    df['paragraphs'] = df.apply(lambda row: fetch_process_warc_records(row, s3client), axis=1)
    print(f'Success! Finished in {time.process_time() - start} seconds.')
    print(f'Share of URLs mentioning at least one keyword: {len(df.paragraphs[df.paragraphs.str.len()>0])/len(df)}')

    # explode so we have one paragraph per row
    df = df[['url_host_name', 'url', 'crawl', 'paragraphs']].explode('paragraphs')

    # save to S3
    s3_path = f's3://{args.output_bucket}/{args.output_path}/batch_n_{batch_n}.csv'
    df.to_csv(s3_path, index=False)
    print(f'Results saved to: {s3_path}')


# take only unique paragraphs - doesn't make sense to do this in each batch, do this at the end instead
# df = df.groupby(['url_host_name', 'url', 'paragraphs']).crawl.apply(set).reset_index()

# to find required number of batches
# import pandas as pd
# chunksize = 10 ** 7
# row_count = 0
# for chunk in pd.read_csv('s3://cc-extract/urls_merged_cc/fc16332b-b3a1-449c-b440-ab0d060fdd08.csv', chunksize=chunksize, usecols=['url']):
#     row_count += len(chunk)
# req_batches = row_count//100 + 1