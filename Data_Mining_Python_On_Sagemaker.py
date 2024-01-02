import pandas as pd
import boto3
import requests
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib as mpl
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from matplotlib.ticker import ScalarFormatter
import matplotlib.ticker as ticker
import uuid

pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

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

# 1.1 What is the current median sale price for properties in the area?
selected_states = ['NY', 'NJ', 'MD', 'VA', 'NC', 'FL', 'CA']
filtered_df = merged_df[merged_df['state_code'].isin(selected_states)]

# Calculate median sale price for each state
median_sale_price_by_state = filtered_df.groupby('state_code')['median_sale_price'].median().reset_index()

# Sort the result in ascending order based on median_sale_price
median_sale_price_by_state = median_sale_price_by_state.sort_values(by='median_sale_price', ascending=True)

#Bar graph
colors = [ 'springgreen', 'springgreen', 'springgreen','limegreen','green', 'green','darkgreen']
plt.figure(figsize=(20, 8))
plt.barh(median_sale_price_by_state['state_code'], median_sale_price_by_state['median_sale_price'], color=colors)
plt.xlabel('Median Sale Price')
plt.ylabel('State Code')
plt.title('Median Sale Price by State')
for i, value in enumerate(median_sale_price_by_state['median_sale_price']):
    plt.text(value, i, f'${value:.2f}', ha='left', va='center')

plt.show()


# 1.2 What is the current inventory level, and how does it compare to historical averages? (Supply)
# Convert 'date' column to datetime format
filtered_df['date'] = pd.to_datetime(filtered_df['date'])

# Filter data for the year 2022
inventory_by_state = filtered_df[filtered_df['date'].dt.year == 2021]

# Calculate average inventory for each state in 2022
average_inventory_by_state = inventory_by_state.groupby('state_code')['inventory'].mean().reset_index()

# Sort the result in ascending order based on inventory
average_inventory_by_state = average_inventory_by_state.sort_values(by='inventory', ascending=True)

# Define custom colors for the bars
colors = [ 'darksalmon', 'darksalmon', 'bisque','yellow','gold', 'green','green']

# Create a bar graph
plt.figure(figsize=(20, 8))
plt.bar(average_inventory_by_state['state_code'], average_inventory_by_state['inventory'], color=colors)
plt.xlabel('State Code')
plt.ylabel('Recent Average Inventory')
plt.title('Recent Average Inventory by State')
plt.show()







# 1.3 What is the housing demand?
# Assuming merged_df is your DataFrame
selected_states = ['NY', 'NJ', 'MD', 'VA', 'NC', 'FL']
filtered_df = merged_df[merged_df['state_code'].isin(selected_states)]

# Convert 'date' column to datetime format
filtered_df['date'] = pd.to_datetime(filtered_df['date'])

# Filter out the year 2022
filtered_df = filtered_df[filtered_df['date'].dt.year != 2022]

# Extract the year from the date
filtered_df['year'] = filtered_df['date'].dt.year

# Group by state and year, calculate the median sale price
median_sale_price = filtered_df.groupby(['state_code', 'year'])['median_sale_price'].median().reset_index()

# Group by state and year, calculate the average inventory
avg_inventory = filtered_df.groupby(['state_code', 'year'])['inventory'].mean().reset_index()

# Group by state and year, calculate the average number of homes sold
avg_homes_sold = filtered_df.groupby(['state_code', 'year'])['homes_sold'].mean().reset_index()

# Create subplots
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(20, 12))

# Plot Median Sale Price
sns.lineplot(x='year', y='median_sale_price', hue='state_code', data=median_sale_price, marker='o', palette='viridis', ax=ax1)
ax1.set_xlabel('Year')
ax1.set_ylabel('Median Sale Price')
ax1.set_title('Median Sale Price Each Year for Selected States')
ax1.legend(title='State Code', loc='upper left')

# Plot Average Inventory
sns.lineplot(x='year', y='inventory', hue='state_code', data=avg_inventory, marker='o', palette='viridis', ax=ax2)
ax2.set_xlabel('Year')
ax2.set_ylabel('Average Inventory')
ax2.set_title('Average Inventory Each Year for Selected States')
ax2.legend(title='State Code', loc='upper left')

# Plot Average Homes Sold
sns.lineplot(x='year', y='homes_sold', hue='state_code', data=avg_homes_sold, marker='o', palette='viridis', ax=ax3)
ax3.set_xlabel('Year')
ax3.set_ylabel('Average Homes Sold')
ax3.set_title('Average Homes Sold Each Year for Selected States')
ax3.legend(title='State Code', loc='upper left')

# Adjust layout
plt.tight_layout()
plt.show()




# 2.1 Does house size (sqft) type affect the house price? 
# Assuming merged_df is your DataFrame
filtered_df = merged_df[(merged_df['sale_price'] < 10000000) & (merged_df['squarefeet'] < 100000)]

plt.figure(figsize=(12, 8))

# Create a custom formatting function for sale price
def format_dollars(value, _):
    return '${:,.0f}'.format(value)

# Apply the custom formatting function to the y-axis
g = sns.lmplot(x='squarefeet', y='sale_price', data=filtered_df, height=8, aspect=1.5, scatter_kws={'color': 'blue'}, line_kws={'color': 'orange'})
g.ax.yaxis.set_major_formatter(FuncFormatter(format_dollars))

# Customize x-axis labels to show square footage
xticklabels = ['{:,}'.format(int(x)) for x in g.ax.get_xticks()]
g.set_xticklabels(xticklabels)

# Set maximum scales
plt.xlim(0, 100000)   # Set maximum x-axis scale for square feet
plt.ylim(0, 10000000)  # Set maximum y-axis scale for sale price

plt.title('Regression Line: House Size vs Sale Price (Sale Price < $10,000,000)')
plt.xlabel('Square Feet')
plt.ylabel('Sale Price')
plt.show()






# 2.2 How do the number of rooms correlate with property prices? 
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# Assuming merged_df is your DataFrame
plt.figure(figsize=(12, 8))

# Create a custom formatting function for sale price
def format_dollars(value, _):
    return '${:,.0f}'.format(value)

# Apply the custom formatting function to the y-axis
sns.scatterplot(x='num_rooms', y='sale_price', data=merged_df)
plt.gca().yaxis.set_major_formatter(FuncFormatter(format_dollars))

# Customize the plot
plt.title('Correlation between Number of Rooms and Property Prices')
plt.xlabel('Number of Rooms')
plt.ylabel('Sale Price')

# Show the plot
plt.show()






# 2.3 Does property type affect the house price? 
plt.figure(figsize=(20, 8))
sns.boxplot(x='property_type', y='sale_price', data=merged_df)
plt.title('Effect of Property Type on House Price')
plt.xlabel('Property Type')
plt.ylabel('Sale Price')

# Set y-axis to log scale
plt.yscale('log')

# Format y-axis labels to display sale prices in dollars
formatter = ScalarFormatter()
formatter.set_scientific(False)
plt.gca().yaxis.set_major_formatter(formatter)

# Set y-axis tick locations manually (adjust as needed)
plt.yticks([10, 1000, 100000, 10000000, 1000000000], ['$10', '$1,000',  '$100,000', '$10,000,000', '$1,000,000,000'])

plt.show()





# 2.4 Does the new house make the house price higher?
# Filter out records with 'nan' values
filtered_df = merged_df[(merged_df['sale_price'] < 1e7) & (merged_df['squarefeet'] < 1e5) & (~merged_df['build_period'].isna())]

# Define the order of build periods
order = ['[-inf, 1950.0)', '[1950.0, 1960.0)', '[1960.0, 1970.0)', '[1970.0, 1980.0)',
         '[1980.0, 1990.0)', '[1990.0, 2000.0)', '[2000.0, 2010.0)', '[2010.0, 2022.0)', '[2022.0, inf)']

# Define descriptive labels for each build period
labels = ['Before 1950', '1950s', '1960s', '1970s', '1980s', '1990s', '2000s', '2010s', 'After 2022']

# Create subplots with 2 rows and 2 columns
fig, axes = plt.subplots(2, 2, figsize=(20, 15))

# Plot for Townhouse
sns.boxplot(x='build_period', y='sale_price', data=filtered_df[filtered_df['property_type'] == 'Townhouse'], order=order, ax=axes[0, 0])
axes[0, 0].set_title('Townhouse - Sale Price Over Time')
axes[0, 0].set_xlabel('Build Period')
axes[0, 0].set_ylabel('Sale Price (in dollars)')
axes[0, 0].set_xticklabels(labels, rotation=45, ha='right')  # Use descriptive labels and rotate x-axis labels

# Plot for Condo/Co-Op
sns.boxplot(x='build_period', y='sale_price', data=filtered_df[filtered_df['property_type'] == 'Condo/Co-op'], order=order, ax=axes[0, 1])
axes[0, 1].set_title('Condo/Co-Op - Sale Price Over Time')
axes[0, 1].set_xlabel('Build Period')
axes[0, 1].set_ylabel('Sale Price (in dollars)')
axes[0, 1].set_xticklabels(labels, rotation=45, ha='right')  # Use descriptive labels and rotate x-axis labels

# Plot for Single Family Residential
sns.boxplot(x='build_period', y='sale_price', data=filtered_df[filtered_df['property_type'] == 'Single Family Residential'], order=order, ax=axes[1, 0])
axes[1, 0].set_title('Single Family Residential - Sale Price Over Time')
axes[1, 0].set_xlabel('Build Period')
axes[1, 0].set_ylabel('Sale Price (in dollars)')
axes[1, 0].set_xticklabels(labels, rotation=45, ha='right')  # Use descriptive labels and rotate x-axis labels

# Plot for Multi-Family (2-4 Unit)
sns.boxplot(x='build_period', y='sale_price', data=filtered_df[filtered_df['property_type'] == 'Multi-Family (2-4 Unit)'], order=order, ax=axes[1, 1])
axes[1, 1].set_title('Multi-Family (2-4 Unit) - Sale Price Distribution Over Time')
axes[1, 1].set_xlabel('Build Period')
axes[1, 1].set_ylabel('Sale Price (in dollars)')
axes[1, 1].set_xticklabels(labels, rotation=45, ha='right')  # Use descriptive labels and rotate x-axis labels

# Set y-axis to log scale
for ax in axes.flat:
    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("${x:,.0f}"))
    ax.set_yticks([10, 1000, 100000, 10000000])
    ax.set_yticklabels(['$10', '$1,000', '$100,000', '$10,000,000'])

# Adjust layout
plt.tight_layout()
plt.show()
