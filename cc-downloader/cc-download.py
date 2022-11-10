import pandas as pd
import boto3
from warcio.archiveiterator import ArchiveIterator
import time
from io import BytesIO
from bs4 import BeautifulSoup, SoupStrainer
import argparse
import awswrangler as wr
import os
import json
from textblob import TextBlob
from urllib.request import urlopen

from passage_extraction import PassageExtractor
from problem_classification import ProblemClassifier
from utils import *


def fetch_process_warc_records(row, s3client, keywords):
    """Fetch all WARC records defined by filenames and offsets in batch,
    parse the records and the contained HTML, return all paragraphs containing at least one of the
    keywords.csv"""

    only_paragraphs = SoupStrainer('p')

    warc_path, offset, end = row.warc_filename, str(row.warc_record_offset), str(row.warc_record_end)

    rangereq = 'bytes={}-{}'.format(offset, end)

    response = exponential_backoff(s3client.get_object, Bucket='commoncrawl', Key=warc_path, Range=rangereq)

    record_stream = BytesIO(response["Body"].read())

    extracts = []
    for record in ArchiveIterator(record_stream):
        page = record.content_stream().read()

        soup = BeautifulSoup(page, 'lxml', parse_only=only_paragraphs)

        text = soup.get_text()

        extractor = PassageExtractor(text, keywords)
        extracts += extractor.extract_relevant_passages()

    return list(set(extracts)) # remove duplicates



if __name__ == "__main__":
    # print('Installing packages...')
    # for package in ['argostranslate', 'langdetect']:
    #     install_import(package)
    # print('Packages installed.')
    #
    # # download textblob corpora
    # print('Downloading textblob corpora...')
    # os.system('python -m textblob.download_corpora lite')
    # print('Corpora downloaded.')
    #
    # # download nltk corpora
    # print('Downloading nltk corpora...')
    # nltk.download('omw-1.4')
    # print('nltk corpora downloaded.')


    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, required=True)
    parser.add_argument("--output_bucket", type=str, required=True)
    parser.add_argument("--result_output_path", type=str, required=True)
    parser.add_argument("--keywords_path", type=str, required=True) # default='https://github.com/jakob-ra/cc-download/raw/main/cc-download/keywords.csv')
    parser.add_argument("--topic_keywords_path", type=str, required=True)
    args = parser.parse_args()

    keywords = pd.read_csv(args.keywords_path).squeeze().tolist()

    if "AWS_BATCH_JOB_ARRAY_INDEX" in os.environ:
        batch_n = os.environ['AWS_BATCH_JOB_ARRAY_INDEX']
        batch_n = int(batch_n)
        print(f'Processing batch {batch_n}.')
    else:
        batch_n = 0
        print('Processing first batch (no array index found).')

    session = boto3.Session(region_name='us-east-1')

    sts = session.client("sts")
    print(sts.get_caller_identity())

    # read cc-index table with warc filenames and byte positions
    query = f'SELECT * FROM urls_merged_cc_to_download OFFSET {batch_n} LIMIT {args.batch_size} '
    df = wr.athena.read_sql_query(sql=query, database="ccindex", boto3_session=session)
    assert len(df) > 1, "Empty input table!"

    # initialize s3
    s3client = boto3.client('s3', region_name='us-east-1', use_ssl=False)

   # download paragraphs and fill into new column
    print('Starting download...')
    start = time.process_time()
    df['paragraphs'] = df.apply(lambda row: fetch_process_warc_records(row, s3client, keywords), axis=1)
    print(f'Success! Finished downloading in {time.process_time() - start} seconds.')
    print(f'Share of domains mentioning at least one keyword: {df.groupby("url_host_registered_domain").paragraphs.apply(lambda x: len(x[x.str.len()>0]) > 0).mean()}')
    print(f'Share of subpages mentioning at least one keyword: {len(df.paragraphs[df.paragraphs.str.len()>0])/len(df)}')

    # drop offsets
    df.drop(columns=['warc_filename', 'warc_record_offset', 'warc_record_end'], inplace=True)

    # save domains without any mentions of keywords
    domains_without_mentions = df[df.paragraphs.str.len() == 0][['url_host_registered_domain', 'crawl']]
    domains_without_mentions.to_csv(f's3://{args.output_bucket}/{args.result_output_path}/domains_without_mentions/domains_without_mentions_{batch_n}.csv', index=False)

    # continue with non-empty domains
    df = df[df.paragraphs.str.len() > 0].copy(deep=True)

    # detect language on first characters of first paragraph
    print('Starting language detection...')
    start = time.process_time()
    df['lang'] = df.paragraphs.str[0].str.strip().str[:50].apply(detect_lang)
    print(f'Success! Finished language detection in {time.process_time() - start} seconds.')

    # explode so we have one paragraph per row
    df = df.explode('paragraphs')
    df.reset_index(drop=True, inplace=True)
    df.rename(columns={'paragraphs': 'paragraph'}, inplace=True)
    df['paragraph'] = df.paragraph.str.strip()
    df = df[df.paragraph.str.len() > 20].copy(deep=True) # drop very short paragraphs

    # save non-english pages to S3
    non_english = df[df.lang != 'en']
    non_english.to_csv(f's3://{args.output_bucket}/{args.result_output_path}/non_english/non_english_{batch_n}.csv', index=False)

    # continue with english pages
    df = df[df.lang == 'en'].copy(deep=True)

    # translation
    # print('Starting translation...')
    # start = time.process_time()
    # df['translated_paragraphs'] = np.nan
    # to_code = 'en'
    # langs = ['de', 'es', 'nl', 'fr', 'pt', 'it', 'ja', 'ru', 'id', 'sv', 'pl']
    # lang_counts = df.lang.value_counts(normalize=True)
    # nonrare_langs = lang_counts[lang_counts > 0.01].index.tolist()
    # langs = [l for l in nonrare_langs if l in langs]
    # for from_code in langs:
    #     print(f'Downloading model {from_code}-{to_code}...')
    #     model_path = download_argos_model(from_code, to_code)
    #     install_argos_model(model_path)
    #     print(f'Loading model {from_code}-{to_code}...')
    #     model = load_argos_model(from_code, to_code)
    #     print(f'Translating {len(df[df.lang == from_code])} paragraphs from {from_code} to {to_code}...')
    #     df.loc[df.lang == from_code, 'translated_paragraphs'] = df[df.lang == from_code].paragraphs.apply(lambda text: argos_translate(model, text))
    # df['translated_paragraphs'] = df.translated_paragraphs.astype(str).str.strip()
    # print(f'Success! Finished translation in {time.process_time() - start} seconds.')

    # problem classification
    print('Starting problem classification...')
    start = time.process_time()
    with urlopen(args.topic_keywords_path) as url:
        topic_keywords = json.load(url)
    problem_classifier = ProblemClassifier(topic_keywords)
    df = pd.concat([df, df['translated_paragraphs'].apply(problem_classifier.classify).apply(pd.Series)], axis=1)
    print(f'Success! Finished problem classification in {time.process_time() - start} seconds.')

    # sentiment analysis
    print('Starting sentiment analysis...')
    start = time.process_time()
    df['sentiment'] = df.translated_paragraphs.apply(str).apply(lambda x: TextBlob(x).sentiment.polarity)
    print(f'Success! Finished sentiment analysis in {time.process_time() - start} seconds.')

    # save to S3
    s3_path = f's3://{args.output_bucket}/{args.result_output_path}/english/batch_n_{batch_n}.csv'
    df.to_csv(s3_path, index=False)
    print(f'Results saved to: {s3_path}')



