from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, when, coalesce
import uuid
from airflow.providers.amazon.transfers.s3_to_s3_copy import S3ToS3CopyOperator
import warnings

pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

def redfin_extract_data(url):
    df_redfin_pandas = pd.read_csv(url, compression='gzip', sep='\t')
    file_str = 'redfin_data'
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    df_redfin_pandas.to_csv(output_file_path, index=False)
    return output_file_path, file_str

def process_redfin_data(df_redfin):
    df_redfin['period_end'] = pd.to_datetime(df_redfin['period_end'])
    df_redfin['month'] = df_redfin['period_end'].dt.month
    df_redfin['year'] = df_redfin['period_end'].dt.year

    selected_columns = ['table_id', 'region', 'state', 'state_code', 'property_type',
                        'median_sale_price', 'median_list_price', 'median_ppsf',
                        'homes_sold', 'pending_sales', 'new_listings', 'inventory',
                        'parent_metro_region', 'month', 'year']
    df_redfin = df_redfin[selected_columns]
    df_redfin['region'] = df_redfin['region'].str.extract('(\d+)')
    df_redfin = df_redfin.rename(columns={'region': 'zip'})

    return df_redfin

def extract_data_spark(url):
    spark = SparkSession.builder \
        .appName("example") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "8g") \
        .getOrCreate()

    file_path = url
    df_zillow_spark = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)
    return df_zillow_spark

def process_zillow_data(df_zillow_spark):
    df_zillow_spark = df_zillow_spark.withColumn("month", month(col("sale_datetime")))
    df_zillow_spark = df_zillow_spark.withColumn("year", year(col("sale_datetime")))

    cols = ['state', 'property_zip5', 'property_street_address', 'property_city', 'property_county',
            'property_type', 'sale_price', 'building_year_built', 'month', 'year']
    df_zillow_spark = df_zillow_spark.select([col(c) for c in cols])

    df_zillow_spark = df_zillow_spark.dropna()

    conditions = [
        (col("property_type").like("%RESIDENTIAL%"), "All Residentials"),
        (col("property_type").like("%SINGLE FAMILY%"), "Single Family Residential"),
        (col("property_type").like("%MULTIFAMILY%"), "Multi-Family (2-4 Unit)"),
        (col("property_type").like("%CONDO%"), "Condo/Co-op"),
        (col("property_type").like("%TOWNHOUSE%"), "Townhouse")
    ]

    df_zillow_spark = df_zillow_spark.withColumn("property_type",
                                                 coalesce(
                                                     when(conditions[0][0], conditions[0][1]),
                                                     when(conditions[1][0], conditions[1][1]),
                                                     when(conditions[2][0], conditions[2][1]),
                                                     when(conditions[3][0], conditions[3][1]),
                                                     when(conditions[4][0], conditions[4][1]),
                                                     col("property_type")
                                                 ))

    property_types_to_keep = ['All Residential', 'Single Family Residential', 'Townhouse', 'Multi-Family (2-4 Unit)', 'Condo/Co-op']
    df_zillow_spark = df_zillow_spark.filter(col("property_type").isin(property_types_to_keep))
    df_zillow_spark = df_zillow_spark.filter(col("sale_price") != 0)

    df_zillow = df_zillow_spark.toPandas()
    return df_zillow

def merge_data(**kwargs):
    # Implement your merging logic here
    redfin_data_path = kwargs['ti'].xcom_pull(task_ids='process_redfin_data')[0]
    zillow_data_path = kwargs['ti'].xcom_pull(task_ids='process_zillow_data')[0]

    df_redfin = pd.read_csv(redfin_data_path)
    df_zillow = pd.read_csv(zillow_data_path)

    
    #Rename columns
    df_zillow = df_zillow.rename(columns={'property_zip5': 'zip'})

    # Merge dataframes
    merged_data = pd.merge(df_zillow, df_redfin, on=['zip', 'month', 'year', 'property_type'], how='inner')

    # Save the merged data to a CSV file
    merged_file_path = "/home/ubuntu/merged_data.csv"
    merged_data.to_csv(merged_file_path, index=False)

    return merged_file_path

# Define URLs
redfin_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'
zillow_url = 'https://www.dolthub.com/csv/dolthub/us-housing-prices/main'

# Define S3 bucket names
redfin_s3_bucket = 'redfin-s3-bucket'
zillow_s3_bucket = 'zillow-s3-bucket'
merged_s3_bucket = 'merged-s3-bucket'

# Define S3 keys
redfin_s3_key = 'redfin_data.csv'
zillow_s3_key = 'zillow_data.csv'
merged_s3_key = 'merged_data.csv'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('redfin_analytics_dag',
        default_args=default_args,
        # schedule_interval = '@weekly',
        catchup=False) as dag:
# Task to extract data from Redfin URL
redfin_extract_task = PythonOperator(
    task_id='redfin_extract_task',
    python_callable=redfin_extract_data,
    op_args=[redfin_url],
    provide_context=True,
    dag=dag,
)

# Task to process Redfin data
process_redfin_task = PythonOperator(
    task_id='process_redfin_task',
    python_callable=process_redfin_data,
    provide_context=True,
    dag=dag,
)

# Task to extract data from Zillow URL
zillow_extract_task = PythonOperator(
    task_id='zillow_extract_task',
    python_callable=extract_data_spark,
    op_args=[zillow_url],
    provide_context=True,
    dag=dag,
)

# Task to process Zillow data
process_zillow_task = PythonOperator(
    task_id='process_zillow_task',
    python_callable=process_zillow_data,
    provide_context=True,
    dag=dag,
)

# Task to copy data to S3
redfin_copy_task = S3ToS3CopyOperator(
    task_id='redfin_copy_task',
    source_bucket_key=f'{redfin_s3_bucket}/{redfin_s3_key}',
    dest_bucket_key=f'{merged_s3_bucket}/{redfin_s3_key}',
    replace=True,
    aws_conn_id='aws_default',
    dag=dag,
)

zillow_copy_task = S3ToS3CopyOperator(
    task_id='zillow_copy_task',
    source_bucket_key=f'{zillow_s3_bucket}/{zillow_s3_key}',
    dest_bucket_key=f'{merged_s3_bucket}/{zillow_s3_key}',
    replace=True,
    aws_conn_id='aws_default',
    dag=dag,
)

# Task to merge data
merge_data_task = PythonOperator(
    task_id='merge_data_task',
    python_callable=merge_data,  # You need to implement the merge_data function
    provide_context=True,
    dag=dag,
)

# Set task dependencies
redfin_extract_task >> process_redfin_task
zillow_extract_task >> process_zillow_task
[process_redfin_task, process_zillow_task] >> [redfin_copy_task, zillow_copy_task] >> merge_data_task
merge_data_task >> upload_to_sagemaker_task

    
