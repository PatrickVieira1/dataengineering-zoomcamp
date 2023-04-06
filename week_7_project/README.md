# Documentation from the Project on Data Engineering Zoomcamp

## Gas Prices from Brazil Dataset

### Problem Description

- I chose a dataset that represents my country and makes sense to me, while having a good size to do some analysis.
- So the main goal was to build a data pipeline that make the batch data goes from a public source to a dashboard.
- But using the most modern tools to have scalability, reproducibility and availability. The data stack chosen is shown below:

  - Data Lake: Google Cloud Storage
  - Pipeline Orchestration: Prefect
  - Data Warehouse: Snowflake
  - Data Transformation: dbt
  - BI tool: Looker Studio

- The main difference from what we learnt on the course, was that I chose Snowflake instead of Bigquery, just to have experience using Snowflake, but they both serve the same purpose.

## Instructions

### To reproduce the steps to bring the data from source to the dashboard you have

#### Google Cloud

- Create a bucket in Google Cloud Storage, create an IAM role with a service account that will be stored in Prefect, so it can access to write it only
- Create another service account that will be used by Snowflake

#### Prefect

- Start prefect server by inputing in the CLI `prefect orion start`
- Configure your block accessing `http://127.0.0.1:4200/` and following the UI
- After that you can run the [elt_web_to_gcs.py](elt_web_to_gcs.py)
- This will download and send all the files from gas prices from 2004 until 2021 to your Google Cloud Storage

#### Snowflake

- To connect your bucket and create a database from your files in the Google Cloud Storage, you have to stage them in Snowflake and just then copy into a table.
- All steps used is written in the file [snowflake.sql](snowflake.sql)
- After you created and copied into your raw_data table you can move to dbt to do sql transformations.

#### dbt

- After connecting to snowflake, you can start modifying yml and sql files from dbt
- Be careful with quotes, because snowflake treat them differently from others databases, so you can pass some parameters in yml files such as `quoting` to reference them corretly.
- After verifying the first table, you can use `dbt run` to execute all files and create the final table that will be used on our dashboard

#### Looker Studio

- Connecting to Snowflake was easy by using a Partner Connector that Snowflake created to easily bring data from tables in Snowflake to Looker
- After inputing your credentials, I was confronted with a size limit from the connector. I could just query a limit of 1000000 rows. My data had more than 21 millions.
- That limit made our data range just from the beginning from 2004 to the final of the same year.
- The final link is accessible [here](https://lookerstudio.google.com/reporting/96761495-0c59-4ca9-be7f-911e79c240bc)

![Image from the dashboard](images/dashboard_image)
