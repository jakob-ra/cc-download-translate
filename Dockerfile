FROM python:3.10.7

WORKDIR /cc-download

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY cc-download ./cc-download

CMD ["python", "./cc-download/cc-download.py"]