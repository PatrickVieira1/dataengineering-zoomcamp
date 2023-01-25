FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2


WORKDIR /app
COPY upload-data.py upload-data.py

ENTRYPOINT [ "python", "upload-data.py" ]