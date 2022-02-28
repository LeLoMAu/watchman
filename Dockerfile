FROM python:3.8-slim-buster

RUN mkdir -p "opt/watchman"
WORKDIR "/opt/watchman"
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY watchman watchman

ENV CACHEBUST=0

COPY /Users/matteozantedeschi/PycharmProjects/watchman/credentials/watchman-316311-2fbf7d0acef8.json watchman-316311-2fbf7d0acef8.json
COPY main_yahoo_finance.py main_yahoo_finance.py

CMD python main_yahoo_finance.py