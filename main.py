from athena_lookup import Athena_lookup
import pandas as pd
import os
import s3fs
from aws_config import aws_config_credentials
from aws_batch import AWSBatch
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

batch_size = 5000


athena_lookup = Athena_lookup(aws_params, s3path_url_list, crawls, n_subpages, url_keywords,
                              limit_cc_table=None, keep_ccindex=True)
athena_lookup.run_lookup()

req_batches = int(athena_lookup.download_table_length//batch_size + 1)
print(f'Splitting {athena_lookup.download_table_length} subpages into {req_batches} batches of size {batch_size}.')

aws_batch = AWSBatch(req_batches, batch_size, output_bucket, output_path, keywords_path)
aws_batch.run()



# estimate aws batch cost
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











## read results
df = pd.concat([pd.read_csv(f's3://{output_bucket}/{output_path}/batch_n_{i}.csv') for i in range(req_batches)])

df = pd.concat([pd.read_csv(f's3://{output_bucket}/cc-download/CC-MAIN-2020-24/batch_n_{i}.csv') for i in range(40)])

# df = pd.read_csv(f's3://{output_bucket}/cc-download/CC-MAIN-2020-16/batch_n_{0}.csv')

from ast import literal_eval
df['paragraphs'] = df.paragraphs.apply(literal_eval)
df = df.explode('paragraphs')
df['paragraphs'] = df.paragraphs.str.strip()
df.drop(columns=['warc_filename', 'warc_record_offset', 'warc_record_end'], inplace=True)



## detect language
from langdetect import detect
def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except:
        return None

df['lang'] = df.paragraphs.str[:50].apply(detect_lang)



## Descriptives language distribution
lang_counts = df.lang.value_counts().to_frame(name='count')
lang_counts['share'] = lang_counts/lang_counts.sum()
lang_counts['cum_share'] = lang_counts.share.cumsum()

lang_codes = pd.read_csv('/Users/Jakob/Downloads/language-codes.csv', names=['lang', 'language'])
lang_codes = lang_codes.append({'lang': 'zh-cn', 'language': 'Chinese'}, ignore_index=True)
lang_counts = lang_counts.reset_index(names='lang').merge(lang_codes, on='lang', how='left')
lang_counts = lang_counts.merge(argos_models, left_on='lang', right_on='from_code', how='left')
lang_counts = lang_counts.iloc[1:] # remove english

import matplotlib.pyplot as plt
import numpy as np
fig, ax = plt.subplots(figsize=(14,6))
lang_counts.share.plot(ax=ax, kind='bar', color='grey')
ax.set_xticklabels(lang_counts.language, rotation=45, ha='right')
for i in range(len(lang_counts)):
    if np.isnan(lang_counts.package_version.iloc[i]):
        ax.get_xticklabels()[i].set_color("red")
plt.grid()
plt.tight_layout()
plt.show()

lang_counts
ax.get_xticklabels()

lang_counts.head(12) # take top 12 languages, 95% of content


import argostranslate.translate

def load_argos_model(from_code, to_code):
    installed_languages = argostranslate.translate.get_installed_languages()
    from_lang = list(filter(lambda x: x.code == from_code, installed_languages))[0]
    to_lang = list(filter(lambda x: x.code == to_code, installed_languages))[0]
    model = from_lang.get_translation(to_lang)

    return model

def argos_translate(model, text):
    try:
        return model.translate(text)
    except:
        return None

model = load_argos_model(from_code, to_code)
df['translated'] = None
df[df.lang == 'de'].paragraphs.iloc[:100].apply(lambda x: argos_translate(model, x))

model.translate()









# get rid of old compute environment
# try:
#     # batch_client.deregister_job_definition(jobDefinition=batch_env_name)
#     batch_client.update_job_queue(jobQueue=batch_env_name, state='DISABLED')
#     time.sleep(2)
#     batch_client.delete_job_queue(jobQueue=batch_env_name)
#     time.sleep(2)
#     batch_client.update_compute_environment(
#         computeEnvironment=batch_env_name,
#         state='DISABLED',
#     )
#     time.sleep(2)
#     batch_client.delete_compute_environment(
#         computeEnvironment='cc-download'
#     )
#     time.sleep(2)
# except Exception as e:
#     print(e)