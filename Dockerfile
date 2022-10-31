FROM python:3.10.7

WORKDIR /cc-download

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN python3 download_install_argos_models.py

COPY cc-download ./cc-download

## the command is submitted directly to AWS batch as a job definition:
# python ./cc-download/cc-download.py --output_bucket cc-extract --output_path cc-download-test --batch_size 100 --keywords https://github.com/jakob-ra/cc-download/raw/main/cc-download/keywords.csv

## to push the dockerfile to Amazon Elastic Container Registry:
# start docker daemon
# docker build -t cc-download .
# docker tag cc-download:latest public.ecr.aws/r9v1u7o6/cc-download:latest
# aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
# docker push public.ecr.aws/r9v1u7o6/cc-download:latest
