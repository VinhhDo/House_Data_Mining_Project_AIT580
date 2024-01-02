# Project: Data Pipeline with Airflow 

## Overview

The project at hand is dedicated to the implementation of ETL (Extract, Transform, Load) pipelines and data mining within the AWS (Amazon Web Services) environment, specifically focusing on the real estate domain. Also, the project leverages datasets from Redfin Real Estate and the Zillow database, providing a foundation for in-depth analysis. Three key pillars form the project's focus:

    Market Conditions and Trends: The analysis scrutinizes current market conditions and trends, offering a nuanced understanding of average sale prices, inventory levels, and broader market trends across different regions.

    Influencing Factors on House Prices: Delving into the intricate factors that can potentially sway house prices, the project aims to equip investors with insights to navigate market fluctuations and make informed decisions.

    Investment Optimization Solutions: By employing data mining techniques, the project seeks to provide actionable solutions for real estate investors. These insights are designed to optimize investment strategies, empowering investors to make strategic choices that align with dynamic market conditions.

## Project Description

This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step. 
We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.
You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

## Datasets
The datasets used in this project are stored within S3 buckets, which can be found under the following URIs:

Log data: `s3://udacity-dend/log_data`
Song data: `s3://udacity-dend/song_data`

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
