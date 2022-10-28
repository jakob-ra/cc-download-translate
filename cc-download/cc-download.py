import botocore.exceptions
import pandas as pd
import boto3
from warcio.archiveiterator import ArchiveIterator
import time
from io import BytesIO
from bs4 import BeautifulSoup, SoupStrainer
import os
import argparse
import awswrangler as wr
import re

def mergeIntervals(arr):
    # Sorting based on the increasing order
    # of the start intervals
    arr.sort(key=lambda x: x[0])

    # Stores index of last element
    # in output array (modified arr[])
    index = 0

    # Traverse all input Intervals starting from
    # second interval
    for i in range(1, len(arr)):

        # If this is not first Interval and overlaps
        # with the previous one, Merge previous and
        # current Intervals
        if (arr[index][1] >= arr[i][0]):
            arr[index][1] = max(arr[index][1], arr[i][1])
        else:
            index = index + 1
            arr[index] = arr[i]

    return arr[:index+1]

def extract_sentences_around_keyword_mention(text: str, keywords: list) -> list:
    sentence_boundary = '(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|!)\s'
    sentences = re.split(sentence_boundary, text)
    intervals = []
    for index, sentence in enumerate(sentences):
        for keyword in keywords:
            if keyword.casefold() in sentence.casefold():
                keyword_mention_index = index
                start_index = max(0, keyword_mention_index - 5)
                end_index = min(len(sentences), keyword_mention_index + 7)
                intervals.append([start_index, end_index])
    merged_intervals = mergeIntervals(intervals)
    relevant_sentences = [' '.join(sentences[start_index:end_index]) for start_index,end_index in merged_intervals]

    return relevant_sentences

def fetch_process_warc_records(row, s3client, keywords, return_paragraphs=False):
    """Fetch all WARC records defined by filenames and offsets in batch,
    parse the records and the contained HTML, return all paragraphs containing at least one of the
    keywords.csv"""

    only_paragraphs = SoupStrainer('p')

    warc_path, offset, end = row.warc_filename, str(row.warc_record_offset), str(row.warc_record_end)

    rangereq = 'bytes={}-{}'.format(offset, end)

    # response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)

    # try-except block to account for request limits
    delay = 1  # initial delay
    delay_incr = 1  # additional delay in each loop
    max_delay = 4  # max delay of one loop. Total delay is (max_delay**2)/2

    while delay < max_delay:
        try:
            response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)
            break

        except botocore.exceptions.ClientError:
            time.sleep(delay)
            delay += delay_incr
    else:
        raise

    record_stream = BytesIO(response["Body"].read())

    relevant_passages = []
    for record in ArchiveIterator(record_stream):
        page = record.content_stream().read()

        soup = BeautifulSoup(page, 'lxml', parse_only=only_paragraphs)

        text = soup.get_text()

        paragraphs = text.split('\n')

        if return_paragraphs == True:
            relevant_passages += [paragraph for paragraph in paragraphs if
                                 any(ext.casefold() in paragraph.casefold() for ext in keywords)]

        else:
            for paragraph in paragraphs:
                relevant_passages += extract_sentences_around_keyword_mention(paragraph, keywords)

    return list(set(relevant_passages))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, required=True)
    parser.add_argument("--output_bucket", type=str, required=True)
    parser.add_argument("--output_path", type=str, required=True)
    parser.add_argument("--keywords_path", type=str, required=True) # default='https://github.com/jakob-ra/cc-download/raw/main/cc-download/keywords.csv')
    args = parser.parse_args()

    keywords = pd.read_csv(args.keywords_path).squeeze().tolist()
    # keywords = pd.read_csv(args.keywords_path).squeeze().tolist()

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
    query = f'SELECT * FROM cc_merged_to_download OFFSET {batch_n} LIMIT {args.batch_size} '
    df = wr.athena.read_sql_query(sql=query, database="ccindex", boto3_session=session)
    assert len(df) > 1, "Empty input table!"

    # df = pd.read_csv(args.download_table_loc, skiprows=range(1, batch_n * args.batch_size),
    #                  nrows=args.batch_size, header=0)

    # initialize s3
    s3client = boto3.client('s3', region_name='us-east-1', use_ssl=False)

   # download paragraphs and fill into new column
    start = time.process_time()
    df['paragraphs'] = df.apply(lambda row: fetch_process_warc_records(row, s3client, keywords), axis=1)
    print(f'Success! Finished in {time.process_time() - start} seconds.')
    print(f'Share of URLs mentioning at least one keyword: {len(df.paragraphs[df.paragraphs.str.len()>0])/len(df)}')

    # explode so we have one paragraph per row
    # df = df[['url_host_name', 'url', 'crawl', 'paragraphs']].explode('paragraphs')

    # drop pages without any paragraphs
    df = df[df.paragraphs.str.len() > 0]

    # save to S3
    s3_path = f's3://{args.output_bucket}/{args.output_path}/batch_n_{batch_n}.csv'
    df.to_csv(s3_path, index=False)
    print(f'Results saved to: {s3_path}')


# take only unique paragraphs - doesn't make sense to do this in each batch, do this at the end instead
# df = df.groupby(['url_host_name', 'url', 'paragraphs']).crawl.apply(set).reset_index()

