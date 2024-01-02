# Project: Data Pipeline with Airflow 

## Overview

The project at hand is dedicated to the implementation of ETL (Extract, Transform, Load) pipelines and data mining within the AWS (Amazon Web Services) environment, specifically focusing on the real estate domain. Also, the project leverages datasets from Redfin Real Estate and the Zillow database, providing a foundation for in-depth analysis. Three key pillars form the project's focus:

The overarching goal is to empower real estate investors with actionable insights derived from AWS-based ETL pipelines and data mining techniques. This involves not only understanding current market conditions and trends but also identifying factors influencing house prices. The project aims to provide a streamlined and efficient approach to data processing and analysis, ensuring that investors can make well-informed decisions in the dynamic real estate market.

![ETL](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/cdbbb20a-95dc-4968-886c-03c5b268b09e)

## Datasets
Redfin API: https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz
Zillow Database: https://www.dolthub.com/repositories/dolthub/us-housing-prices-v2/data/main

## Objective
1. Identifying the current market conditions and trends in the real estate industry.
2. Analyzing factors that can potentially influence house prices in the market.
3. Providing solutions for real estate investors to optimize their investments based on insights from data mining.


## Data Warehouse

![Data_Warehouse](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/d0780ed2-90e5-41c1-8ab3-ec40f5221ca2)


## Exploratory Data Analysis

### Major Question 1: What are the current market conditions and trends? 
#### What is the current median sale price for properties in the area?
![1](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/90330d3c-ac9a-4780-bfdf-96c0146aa496)
Analysis: Determining the price of each state enables investors to understand trends and optimize alternative investments. According to the Redfin report, the median home price in the United States is $329,000. New York State has the highest median sale price at $975,000, significantly higher than other states. California and Virginia have prices slightly above the median, at $435,000 and $404,999, respectively. New Jersey closely aligns with the median home price in the U.S., while Florida, North Carolina, and Maryland have median prices far below the national average.


#### What is the current inventory level, and how does it compare to historical averages? (Supply)
![2](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/b5ed1f10-bdbd-47e6-850d-db2a74029d50)
Analysis:  Inventory reflects market trends, showing the supply and demand side. New York and Florida have average inventories above 130 houses, while Virginia, California, and New Jersey fall in the mid-range with 40-80 houses per zip code. However, North Carolina and Maryland face a low supply, with less than 20 houses on average.


#### What is the housing demand over the years?
![3](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/cccc812c-9958-450d-944a-93989e9e85cc)

Analysis: Examining median sale price, average inventory, and average homes sold over time reveals effective indicators for understanding housing demand. New York shows a drastic increase in house prices since 2014, with high inventory and home sales. Florida exhibits an average house price but a high percentage of home sales, indicating a growing trend. Maryland and North Carolina's real estate markets lag behind, with low inventory and stable prices over the past decade.



### Major Question 2: What are the main causes affecting the house price?
#### Does house size (sqft) type affect the house price?
![4](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/93951e1e-1213-49e9-acf8-0d4c5a20dfd5)
Analysis: The bigger size tend to increase the value of the house. Therefore, the house size is important factor to determine when comparing the sale price of house in The United States. Based on the linear, there is a strong linear relationship between two variable house size (square feet) and the home sale price. As a result the house price increase strongly depent on size of house (square feet).


#### How do the number of bedrooms and bathrooms correlate with property prices?
![5](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/bec9e7ad-c65d-4db4-80a5-61eae9f5a9f9)
Analysis: Normally, the house with more rooms tend to have higher price. However, the graph enable audience to see a clear picture about property price by the number of rooms. Even though the graph does not show specific the amount of room and the overlap can affect the visualization, the spread of the data point still can help the audience see different  between each number of room segments. According to graph, there is a positive relationship between those variable. To have a value over 10 millions dolar, a house should have at least 6 rooms. Beside that, the room with 1 room is barely have value over 1 million dollar. 


#### Does property type affect the house price?
![6](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/0ca8ed76-66d4-410a-aa3e-d5ea9547dbaa)
Analysis: Property type significantly influences house prices. The graph significantly show the average line increase of the house sale price from townhouse to single family residential. For single family residential, the average fall into about $300.000 which is higher then Condo $50.000. And, the townhouse have less than others which is only ~$200.000.

#### Does the new house make the house price higher?
![7](https://github.com/VinhhDo/House_Data_Mining_Project_AIT580/assets/98499217/252e0bae-6211-48c8-96e1-55df3b108a88)
Analysis: New houses can have more value by their new roof, floor, and new design style. And, it can be considered to determine when buying a house for investment. Contrary to expectations, the graphs reveal that the building year does not strongly correlate with house prices for condo, single-family, and multi-family residences. For Condo, Single family, and Multi-family residence, they all remain the same price with a light difference between the sale price regarding to build year. The single famality residence and condo fall in the $300.000 range. Townhouses show a slight price increase over decades by increasing $20,000 over decades. However there is no strong correlation between sale price and building price. 


### Results and Insights
#### Market Conditions and Trends in states in The U.S
Florida and Virginia on the east coast have seen a 200% growth in median house prices since 2012. High inventory levels in these states make it easier to find and sell homes, presenting a bright outlook for investors. New York has the highest house price in the U.S., up 300% in 2014, with prices increasing slightly each year. Despite high inventory, it poses higher risks with a need for a considerable capital investment. North Carolina and Maryland have been the least favorable investments, with median house prices below $200,000 remaining for a decade.

#### Factors Affecting House Prices
Square footage and the number of rooms positively correlate with sale prices.
In property type, the town house need least capital to invest which need only $100,000 in east coast compared to Condo and Single Family Residential
The building year does affect the price, but it only affects house building in the 90s and after. But, the old house have higher chance to repairing cost. The house have built from 1970 to 1990 is the best alternative.
