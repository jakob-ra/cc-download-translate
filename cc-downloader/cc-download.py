import pandas as pd
import boto3
from warcio.archiveiterator import ArchiveIterator
import time
from io import BytesIO
from bs4 import BeautifulSoup, SoupStrainer
import argparse
import awswrangler as wr
import numpy as np
import os
import json

from passage_extraction import PassageExtractor
from problem_classification import ProblemClassifier
from utils import install_import


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

    return extracts



if __name__ == "__main__":
    print('Installing packages...')
    for package in ['argostranslate', 'langdetect', 'spacy', 'spacytextblob']:
        install_import(package)
    print('Packages installed.')
    import spacy

    print('Downloading Spacy models...')
    # download spacy models if not already installed
    spacy_model = 'en_core_web_trf'
    if not spacy.util.is_package(spacy_model):
        spacy.cli.download(spacy_model)
    print('Spacy models downloaded.')

    from utils import exponential_backoff, download_argos_model, install_argos_model, load_argos_model, \
        argos_translate, detect_lang, sentiment_analysis_spacy

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, required=True)
    parser.add_argument("--output_bucket", type=str, required=True)
    parser.add_argument("--result_output_path", type=str, required=True)
    parser.add_argument("--keywords_path", type=str, required=True) # default='https://github.com/jakob-ra/cc-download/raw/main/cc-download/keywords.csv')
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
    print(f'Share of domains mentioning at least one keyword: {df.groupby("url_host_registered_domain").paragraphs.apply(lambda x: len(x[x.str.len()>0])).mean()}')
    print(f'Share of subpages mentioning at least one keyword: {len(df.paragraphs[df.paragraphs.str.len()>0])/len(df)}')

    # drop offsets
    df.drop(columns=['warc_filename', 'warc_record_offset', 'warc_record_end'], inplace=True)

    # drop pages without any paragraphs
    df = df[df.paragraphs.str.len() > 0].copy(deep=True)

    # detect language on first characters of first paragraph
    print('Starting language detection...')
    start = time.process_time()
    df['lang'] = df.paragraphs.str[0].str.strip().str[:50].apply(detect_lang)
    print(f'Success! Finished language detection in {time.process_time() - start} seconds.')

    # explode so we have one paragraph per row
    df = df.explode('paragraphs')

    # translation
    print('Starting translation...')
    start = time.process_time()
    df['translated_paragraphs'] = np.nan
    to_code = 'en'
    langs = ['de', 'es', 'nl', 'fr', 'pt', 'it', 'ja', 'ru', 'id', 'sv', 'pl']
    langs = [l for l in df.lang.unique() if l in langs]
    for from_code in langs:
        # print(f'Downloading model {from_code}-{to_code}...')
        # download_install_argos_model(from_code, to_code)
        print(f'Loading model {from_code}-{to_code}...')
        model = load_argos_model(from_code, to_code)
        print(f'Translating {len(df[df.lang == from_code])} paragraphs from {from_code} to {to_code}...')
        df.loc[df.lang == from_code, 'translated_paragraphs'] = df[df.lang == from_code].paragraphs.apply(lambda text: argos_translate(model, text))
    df['translated_paragraphs'] = df.translated_paragraphs.astype(str).str.strip()
    print(f'Success! Finished translation in {time.process_time() - start} seconds.')

    # problem classification
    print('Starting problem classification...')
    start = time.process_time()
    with open('cc-downloader/topic_keywords.json', 'r') as f:
        topic_keywords = json.load(f)
    problem_classifier = ProblemClassifier(topic_keywords, spacy_model=spacy_model)
    df = pd.concat([df, df['translated_paragraphs'].apply(problem_classifier.classify).apply(pd.Series)], axis=1)
    print(f'Success! Finished problem classification in {time.process_time() - start} seconds.')

    # sentiment analysis
    print('Starting sentiment analysis...')
    start = time.process_time()
    nlp = spacy.load(spacy_model)
    nlp.add_pipe('spacytextblob')
    df['sentiment'] = df.translated_paragraphs.apply(str).apply(lambda x: sentiment_analysis_spacy(x, nlp))
    print(f'Success! Finished sentiment analysis in {time.process_time() - start} seconds.')

    # save to S3
    s3_path = f's3://{args.output_bucket}/{args.result_output_path}/batch_n_{batch_n}.csv'
    df.to_csv(s3_path, index=False)
    print(f'Results saved to: {s3_path}')



