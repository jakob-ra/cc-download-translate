FROM python:3.10.7

WORKDIR /cc-download

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY cc-download ./cc-download

#CMD ["python", "./cc-download/cc-download.py"]
# python ./cc-download/cc-download.py --batch_size=5000
# change ecsTaskExecutionRole to have S3 access
# include cchardet lxml job role configuration