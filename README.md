# Project: Data Pipeline with Airflow 

## Overview

The project at hand is dedicated to the implementation of ETL (Extract, Transform, Load) pipelines and data mining within the AWS (Amazon Web Services) environment, specifically focusing on the real estate domain. Also, the project leverages datasets from Redfin Real Estate and the Zillow database, providing a foundation for in-depth analysis. Three key pillars form the project's focus:

The overarching goal is to empower real estate investors with actionable insights derived from AWS-based ETL pipelines and data mining techniques. This involves not only understanding current market conditions and trends but also identifying factors influencing house prices. The project aims to provide a streamlined and efficient approach to data processing and analysis, ensuring that investors can make well-informed decisions in the dynamic real estate market.

![ETL](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/cdbbb20a-95dc-4968-886c-03c5b268b09e)

## Datasets
Redfin API: https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz
Zillow Database: https://www.dolthub.com/repositories/dolthub/us-housing-prices-v2/data/main

## File descriptions

The project has a main directory, `airflow`, which contains two further directories named `dags` and `plugins`. 

`dags` directory contains:
- `udac_example_dag.py`: Defines main DAG, tasks and link the tasks in required order.
- `create_tables.sql`: SQL create table statements provided with template.

`plugins/operators` directory contains:
- `stage_redshift.py`: Defines `StageToRedshiftOperator` to copy JSON data from S3 to staging tables in the Redshift via `copy` command.
- `data_quality.py`: Defines `DataQualityOperator` to run data quality checks on all tables passed as parameter
- `load_dimension.py`: Defines `LoadDimensionOperator` to load a dimension table from staging table(s)
- `load_fact.py`: Defines `LoadFactOperator` to load fact table from staging table(s)
- `sql_queries.py`: Contains SQL queries for the ETL pipeline

## Configurations

- Create a Redshift cluster in `us-west2` 
- Within the Udacity workspace terminal, run the following command: `/opt/airflow/start.sh` and subsequently connect with the Airflow UI
- Within Airflow, add the following two connections:
    - AWS credentials, named `aws_credentials`
    - Connection to Redshift, named `redshift`
- The DAG named `udac_example_dag` should be visible in the UI - click on run
