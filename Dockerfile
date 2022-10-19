FROM python:3.10.7

WORKDIR /cc-download

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY cc-download ./cc-download

#CMD ["python", "./cc-download/cc-download.py"]
# python ./cc-download/cc-download.py --batch_size=10000
# change ecsTaskExecutionRole to have S3 access
# include cchardet lxml job role configuration

# start docker daemon
# docker build -t cc-download .
# docker tag cc-download:latest public.ecr.aws/r9v1u7o6/cc-download:latest
# aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
# docker push public.ecr.aws/r9v1u7o6/cc-download:latest