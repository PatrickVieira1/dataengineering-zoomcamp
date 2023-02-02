FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY 02_gcp /opt/prefect/flows
COPY 03_flow /opt/prefect/flows
COPY data /opt/prefect/data
