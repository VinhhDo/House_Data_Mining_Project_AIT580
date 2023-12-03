#processing libraries
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
import requests
from airflow.operators.bash_operator import BashOperator
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, when, coalesce

pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

url =  'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz'

def redfin_extract_data(url):
    df_redfin_pandas = pd.read_csv(url, compression='gzip', sep='\t')
    file_str = 'redfin_data'
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    df_redfin_pandas.to_csv(output_file_path, index=False)
    return [output_file_path, file_str]

def process_redfin_data(df_redfin):
    # Create new columns for month and year
    df_redfin['period_end'] = pd.to_datetime(df_redfin['period_end'])  # Convert 'period_end' to datetime
    df_redfin['month'] = df_redfin['period_end'].dt.month  # Extract month
    df_redfin['year'] = df_redfin['period_end'].dt.year    # Extract year

    # Keep only the specified columns
    selected_columns = ['table_id', 'region', 'state', 'state_code', 'property_type',
                        'median_sale_price', 'median_list_price', 'median_ppsf',
                        'homes_sold', 'pending_sales', 'new_listings', 'inventory',
                        'parent_metro_region', 'month', 'year']
    df_redfin = df_redfin[selected_columns]
    df_redfin['region'] = df_redfin['region'].str.extract('(\d+)')

    # Rename columns
    df_redfin = df_redfin.rename(columns={'region': 'zip'})

    return df_redfin

)


file_path_Zillow = 'C:/Users/Ben/Desktop/AIT580/Project/dolthub_us-housing-prices_main_sales.csv'

def extract_data_spark(url):
    # Create a Spark session with increased driver memory
    spark = SparkSession.builder \
    .appName("example") \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "8g") \
    .getOrCreate() 
    # Specify the path to the CSV file
    csv_file_path = url
    # Read the CSV file into a PySpark DataFrame
    df_zillow_spark = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(csv_file_path)
    return df_zillow_spark

def process_zillow_data(df_zillow_spark):
    # Create new columns for month and year
    df_zillow_spark = df_zillow_spark.withColumn("month", month(col("sale_datetime")))
    df_zillow_spark = df_zillow_spark.withColumn("year", year(col("sale_datetime")))

    # Select specific columns using Spark functions
    cols = ['state', 'property_zip5', 'property_street_address', 'property_city', 'property_county',
            'property_type', 'sale_price', 'building_year_built', 'month', 'year']
    df_zillow_spark = df_zillow_spark.select([col(c) for c in cols])

    # Drop rows with null values
    df_zillow_spark = df_zillow_spark.dropna()

    # Define the conditions for property_type mapping
    conditions = [
        (col("property_type").like("%RESIDENTIAL%"), "All Residentials"),
        (col("property_type").like("%SINGLE FAMILY%"), "Single Family Residential"),
        (col("property_type").like("%MULTIFAMILY%"), "Multi-Family (2-4 Unit)"),
        (col("property_type").like("%CONDO%"), "Condo/Co-op"),
        (col("property_type").like("%TOWNHOUSE%"), "Townhouse")
    ]

    # Use coalesce to handle multiple conditions and preserve the original 'property_type' if no condition is met
    df_zillow_spark = df_zillow_spark.withColumn("property_type",
                                                 coalesce(
                                                     when(conditions[0][0], conditions[0][1]),
                                                     when(conditions[1][0], conditions[1][1]),
                                                     when(conditions[2][0], conditions[2][1]),
                                                     when(conditions[3][0], conditions[3][1]),
                                                     when(conditions[4][0], conditions[4][1]),
                                                     col("property_type")
                                                 ))

    # Filter the DataFrame based on the list of property types
    property_types_to_keep = ['All Residential', 'Single Family Residential', 'Townhouse', 'Multi-Family (2-4 Unit)', 'Condo/Co-op']
    df_zillow_spark = df_zillow_spark.filter(col("property_type").isin(property_types_to_keep))

    # Remove $0 value house
    df_zillow_spark = df_zillow_spark.filter(col("sale_price") != 0)

    # Transform to Pandas
    df_zillow = df_zillow_spark.toPandas()
    return df_zillow

def extract_data_from_s3_to_sagemaker(s3_bucket, s3_key, local_file, sagemaker_instance, sagemaker_path):
    s3 = boto3.client('s3')
    s3.download_file(s3_bucket, s3_key, local_file)

    sagemaker = boto3.client('sagemaker')
    sagemaker.upload_data(path=sagemaker_path, bucket=sagemaker_instance, key_prefix='', body=open(local_file, 'rb'))

# Example usage:
extract_data_from_s3_to_sagemaker('redfin_transformed_data', 'your_data_file.csv', 'local_data.csv', 'your-sagemaker-instance-name', 'sagemaker/input'
