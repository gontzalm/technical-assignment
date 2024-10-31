FROM python

WORKDIR /app

COPY conf conf/
COPY pipelines pipelines/
COPY scripts scripts/
COPY requirements.txt .

RUN python -m pip install -r requirements.txt
