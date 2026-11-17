"""
PYSPARK ETL pipeline for CoiGecko Cryptocurreccy Data

Purpose :- 
Extract cryptocurrency data from CSV , analyze it , clean invalid records, apply business logic 
, calculate_market statistics , validate data equality and save the final results 
ETL = Extract + Tranform + Load 
"""

import os
import sys

# # Apne Hadoop folder ka path yahan specify karein (jaise C:\hadoop)
# os.environ['HADOOP_HOME'] = r"C:\hadoop"
# sys.path.append(os.path.join(os.environ['HADOOP_HOME'], "bin"))


#  Import Sparksession create and manage a spark application 
import os
from pyspark.sql import SparkSession

# import spark sql functions such as col, when, sum, mean and count
from pyspark.sql import functions as F

# Import spark data types for defining schema when needed 22
from pyspark.sql.types import *

# Import datetime for data and time operations
from datetime import datetime 


# Force Spark to use localhost instead of the computer hostname
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# # Set this only if Hadoop is installed at this location
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
if not os.path.isfile(os.path.join(hadoop_bin, "hadoop.dll")):
    raise FileNotFoundError(
        f"Hadoop native library not found at {os.path.join(hadoop_bin, 'hadoop.dll')}"
    )

path_entries = os.environ.get("PATH", "").split(os.pathsep)
if hadoop_bin.lower() not in (entry.rstrip("\\/").lower() for entry in path_entries):
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
# ================================================================================
# STEP : 1  Initilize spark session  
# ================================================================================

print('PYSPARK ETL PIPELINE FOR COINGECK DATA')

print("\n [STEP 1] Initlizing Spark session....")

# create a spark application named CoinGeckoETL

# Adaptive Query Execution helps spark optimize query execution at run time 
#  Partition coalescing can combine small partition to reduce overhead
spark = SparkSession.builder.appName("CoinGeckoETL") \
    .config("spark.sql.adaptive.enabled","true")\
    .config("spark.driver.host", "127.0.0.1")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()

print("Spark session created sucessfully ")
print(f"Spark version : {spark.version}")

# ================================================================================
# STEP : 2  EXTRACTION - LOAD RAW DATA  
# ================================================================================

print("\n  [STEP 2] EXTRACTION - Loading data from csv....")

df = spark.read.csv('input/coingecko_raw_data.csv',header=True , inferSchema=True)

# Display basic information about the extract data
print("Data Loaded sucessfully")
print(f"Schema : {df.schema}")
print(f"Total records : {df.count()}")

# cache the dataframe because it will be reuse in multiple operation 
#  cache data may reduce read and recomputation 
df.cache()

# ================================================================================
# STEP : 3 EDA(exploratory data analysis)   
# ================================================================================

print("\n [STEP 3] EXPLORATORY DATA ANALYSIS ..")
# COUNT ROWS AND INDENTIFY THE NUMBER OF ROW

print(f"Data Shape {df.count()}  rows , {len(df.columns)} columns")

df.printSchema()

# calculate descrption statistics for supportedd column
# Example include count ,mean , standrad deviation , minimum and maximum

print("\n Basic Statistics : ")
df.describe().show()

# count null values seperatly for every column
print("\n missing values per column: ...")

df.select([
    F.count(
        F.when(F.col(c).isNull(), 1)
    ).alias(c)
    for c in df.columns
]).show()

# ================================================================================
# STEP : 4 DATA CLEANING    
# ================================================================================

print("\n [STEP 4] DATA CLEANING..")

# ===============================
#  4.1 cast correct data types
# ===============================

double_columns = [
    "current_price",
    "total_volume",
    "high_24h",
    "low_24h",
    "price_change_24h",     
    "price_change_percentage_24h",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "ath",
    "atl"
]

for c in double_columns:
    df = df.withColumn(c , F.col(c).cast("double"))


# Convert integer and long columns separately:
df = df.withColumn(
    "market_cap_rank",
    F.col("market_cap_rank").cast("integer")
)

df = df.withColumn(
    "market_cap",
    F.col("market_cap").cast("long")
)

numeric_cols = [
    "price_change_24h",
    "market_cap_change_24h",
    "market_cap_change_percentage_24h"
]

for c in numeric_cols:
    df = df.withColumn(
        c,
        F.col(c).cast("double")
    )

# ===============================
#  4.1 handle missing values 
# ===============================

print("Handling missing values......")

#  Replace NULL values in selected numeric column with zero 
df_clean = df.na.fill({
                "current_price":0,
                "market_cap":0,
                "total_volume":0,
                "circulating_supply":0
})

#  If max_supply is NULL , use circulating_supply as a fallback

df_clean = df_clean.withColumn(
            "max_supply",
            F.coalesce(
                F.col("max_supply"),
                F.col("circulating_supply")
            )
)

# inputCols → Jin columns ke NULL fill karne hain.
# outputCols → Output mein columns ke names.
# setStrategy("median") → Median value se NULL replace karega.
# fit(df) → Har column ka median calculate karega.
# transform(df) → NULL values fill karega.

from pyspark.ml.feature import Imputer

cols = [
    "price_change_24h",
    "market_cap_change_24h",
    "market_cap_change_percentage_24h"
]

imputer = Imputer(
    inputCols=cols,
    outputCols=cols
).setStrategy("median")

df_clean = imputer.fit(df).transform(df)


# ===============================
#  4.2 remove duplicates records 
# ===============================

print("REMOVING DUPLICATE RECORDS.....")

# COUNT RECORDS BEFOR DUPLICATES REMOVAL 

initial_count = df_clean.count()

# remove rows that have identical values across all columns 

df_clean = df_clean.dropDuplicates()

# count records after duplicate 
final_count = df_clean.count()

print(f"Duplicates removed : {initial_count - final_count}")

# ===============================
#  4.3 REMOVE INVALID PRICE  
# ===============================

print("Removing records with invalid price....")

#  count records before filtering 
initial_count = df_clean.count()

#  Keep only records whose current price is greater then zero 

df_clean = df_clean.filter(F.col('current_price')>0)

# count records after filtering 

final_count = df_clean.count()

print(f"Invalid price records removed : {initial_count - final_count}")

# Display the total number of records remaining after cleaning
print(f"Records after cleaning : { df_clean.count()}")

# ================================================================================
# STEP : 5  DATA TRANSFORMATION - APPLY BUSINESS LOGIC   
# ================================================================================

print("\n [STEP 5] DATA TRANSFORMATION.....")

# ===============================
#  5.1 : CREATE DERIVED COLUMNS  
# ===============================

# Calculate the ration of current price to market capitalization 
# adding 1  to the denomination avoid division by zero

df_clean = df_clean.withColumn(
    "price_to_market_cap_ratio",
    F.col("current_price") / (F.col("market_cap")+1)
)

# Calculate trading volume relative to market capitalization 
# This ratio can help compare trading activity across cryptocurrencies
df_clean = df_clean.withColumn(
    "volnme_to_market_cap_ratio",
    F.col("total_volume") / (F.col("market_cap")+1)
) 

# ===============================
#  5.2 CATEGORIZE CRYPTOCURRECIES BY MARKET CAP
# ===============================

print("Categorizing coins by market cap...")

df_clean = df_clean.withColumn(
            "market_cap_category",
            F.when(
                F.col("market_cap") > 1000000000, "Large_cap").when(
                    F.col("market_cap")> 10000000, "Mid_cap").when(
                        F.col("market_cap") > 1000000 ,"Small_cap"
                    ).otherwise("Micro_cap")
        
)



# ===============================
#  5.3 CATEGORIZE 24- hours PRICE CHAGES
# ===============================

print("Categorizing price change....")

# classify coins according to theri 24-hours price change percentage
df_clean = df_clean.withColumn(
    "price_change_category",
    F.when(
        F.col("price_change_percentage_24h") >=5,
        "Strong_Positive"
    ).when(
        F.col("price_change_percentage_24h") >= 0,
        "Positive"
    ).when(
        F.col("price_change_percentage_24h") >=-5,
        "Negetive"
    ).otherwise("Strong Negetive")
)

# ===============================
#  5.4 EXTRACT DATA COMPONENT 
# ===============================

# Convert ath_data into spark timestamp
# ATH mean ALL time High 

df_clean = df_clean.withColumn(
    "ath_date",
    F.to_timestamp("ath_date")
)

# Extract the year from the all time high date 

df_clean = df_clean.withColumn(
    "ath_year",
    F.year("ath_date")
)

df_clean = df_clean.withColumn(
    "ath_month",
    F.month("ath_date")
)

print("Data transformation completed sucessfully...")

# ================================================================================
# STEP 6 DATA AGGREGATION
# ================================================================================

print("\n [STEP 6] DATA AGGREGATION.....")

# -----------------------------------------------
# 6.1 CALCULATE STATISTICS BY MARKET CAP CATEGORY
# -----------------------------------------------

print("Aggregating data by market cap category....")

# group by create group of cryptocurrecies with the same category 
# agg calcutes summary statistics for each group

category_stats = df_clean.groupBy(
    "market_cap_category").agg(
        F.mean("current_price").alias("avg_price"),
        F.median("current_price").alias("median_price"),
        F.stddev("market_cap").alias("std_dev_price"),
        F.sum("market_cap").alias("total_market_cap"),
        F.count("*").alias("coin_count"),
        F.sum("total_volume").alias("total_volume")
    )

print("\n category statistics :")
category_stats.show()

# -----------------------------------------------
# 6.2  FIND THE TOP 10 COINS BY MARKET CAPITALIIZATION
# -----------------------------------------------

print("\n finding top 10 cryptocurrences by market cap....")

# sort market capitalization in descending  order 
# limit(10) returns only the first 10 records 

top_10 = df_clean.orderBy(
    F.desc("market_cap")
).limit(10)

top_10.select(
    "name",
    "symbol",
    "current_price",
    "market_cap").show()

# ===========================================================================================
# STEP 7 : data validataion 
# ===========================================================================================

print("\n [STEP 7]  DATA VALIDATION.....")

# ----------------------------------------------------
# 7.1 CHECK FOR NEGETIVE PRICES
# ----------------------------------------------------

# COUNT rows where the current price is below zero

negetive_price = df_clean.filter(
    F.col("current_price") <0
).count()

# print a warning if any negetive prices are found

if negetive_price < 0:
    print(f"Warning : Found {negetive_price} records with negetive prices")
else :
    print("No Negetive prices found")

# ----------------------------------------------------
# 7.2 CHECK FOR zero market capital with positive price
# ----------------------------------------------------

# Find records where market cap is zero current price is positive

zero_mcap = df_clean.filter(
    (F.col("market_cap") == 0) &
     (F.col("current_price") > 0) ).count()


# report the number of potentially inconsistent record 

if zero_mcap > 0 :
    print(f"Warning : found {zero_mcap} records with zero market cap ...... 'but positive price' ")
else:
    print("NO Zero-Market")

# ===========================================================================================
# STEP 8 : DATA LOADING : SAVE PROCESSED DATA
# ===========================================================================================

print("\n [STEP 8 lOADING - saving cleaned data....]")

# ---------------------------------
# SAVED CLEAD DATA
# ---------------------------------

output_path = "output/coingecko_cleaned_pyspark"

df_clean.write.mode("overwrite").csv(
    output_path,
    header=True
)

print(f"Cleaned data saved to : {output_path}")

# ------------------------------------------
# 8.2 SAVE AGGREGATE STATISTICS
# ------------------------------------------

agg_output_path = "output/coingecko_aggregate_pyspark"

category_stats.write.mode("overwrite").csv(
    agg_output_path,
    header=True
)

print(f"Aggregate data saved to :{agg_output_path}")

# ------------------------------------------
# 8.3 SAVE THE TOP 10 CRYPTOCURRENCIES 
# ------------------------------------------

top_10_output_path = "output/top10_coins_pyspark"

top_10.select(
    "name",
    "symbol",
    "current_price",
    "market_cap"
).write.mode("overwrite").csv(
    top_10_output_path,
    header=True
)

print(f"Top 1- coins saved to : {top_10_output_path}")

# ===================================================================================
#  STEP 9 : PERFORMANCE MONITORING
# ===================================================================================

print("\n [STEP 9] PERFORMANCE MONITORING....")

# Display the number of partition in the cleaned DataFrame
# Partitions are units of work spark can process in the parellel
# R — Resilient Agar koi partition fail ho jaye, Spark usse dobara calculate karke recover kar sakta hai using lineage.
# D — Distributed Data multiple partitions mein divide hokar different machines par process ho sakta hai.
# D — Dataset  Data ka collection, jaise numbers, strings, ya records.

print(f" Number of Partitions : {df_clean.rdd.getNumPartitions()}")

print("\n Query plan :")
df_clean.explain()


# =========================================================================
# SUMMARY 
# =========================================================================

print("PYSPARK ETL PIPELIEE SUMMARY")
print(f"Original RECORDS : {df.count()}")
print(f"Cleaned records :{df_clean.count()}")
print(f"Records removed : {df.count()- df_clean.count()}")
print(f"Processing mode : Distributed computing ")
print(f"Number of partitions : {df_clean.rdd.getNumPartitions()}")
print(f"output files : 3 csv (parittioned)) ")


spark.stop()
print("Spark session stopped...")



