import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import time
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import col, year, month, when, coalesce
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib as mpl
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from matplotlib.ticker import ScalarFormatter
import matplotlib.ticker as ticker
import uuid

#Rename columns
df_zillow = df_zillow.rename(columns={'property_zip5': 'zip'})

# Convert 'sale_price' column to float
merged_df['sale_price'] = merged_df['sale_price'].astype(float)

# Convert 'building_year_built' column to datetime
merged_df['building_year_built'] = pd.to_datetime(merged_df['building_year_built'], errors='coerce')

# Create a new 'date' column by combining 'month' and 'year' columns
merged_df['date'] = pd.to_datetime(merged_df[['year', 'month']].assign(day=1))

# Drop the original 'month' and 'year' columns if needed
merged_df = merged_df.drop(['month', 'year'], axis=1)

# Assuming merged_df is your DataFrame
merged_df['squarefeet'] = merged_df['sale_price'] / merged_df['median_ppsf']

# Assuming merged_df is your DataFrame
merged_df['num_rooms'] = (merged_df['squarefeet'] / 500).apply(np.ceil).fillna(0).astype(int)
room_mapping = {1:1,2:2,3:3,4:4,5:5,6:5,7: 5, 8: 6, 9: 6, 10: 7, 11: 7, 12: 8, 13: 7}

# Apply the mapping to the 'num_rooms' column
merged_df['num_rooms'] = merged_df['num_rooms'].map(room_mapping)
merged_df['building_year_built'] = pd.to_datetime(merged_df['building_year_built'], errors='coerce')

# Define the bins
bins = [float('-inf'), 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2022, float('inf')]

# Create the build_period column
merged_df['build_period'] = pd.cut(merged_df['building_year_built'].dt.year, bins=bins, right=False)

# Convert the build_period column to object type
merged_df['build_period'] = merged_df['build_period'].astype(str)
# Filter out records with sale_price above 20,000,000 or below 10,000
merged_df = merged_df[(merged_df['sale_price'] <= 20000000) & (merged_df['sale_price'] >= 10000)]
