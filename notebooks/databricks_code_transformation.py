# Databricks notebook source
spark


# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC # **Reading the Data**

# COMMAND ----------

csv_path = "abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/bronze/olist_customers_dataset.csv"

df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

display(df)

# COMMAND ----------

geolocation_df = (spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/bronze/olist_geolocation_dataset.csv")
)
display(geolocation_df)

items_df = (spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/bronze/olist_order_items_dataset.csv")
)
display(items_df)

payments_df = (spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/bronze/olist_order_payments_dataset.csv")
)
display(payments_df)

reviews_df = (spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/bronze/olist_order_reviews_dataset.csv")
)
display(reviews_df)

orders_df = (spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/bronze/olist_orders_dataset.csv")
)
display(orders_df)

products_df = (spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/bronze/olist_products_dataset.csv")
)
display(products_df)

sellers_df = (spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/bronze/olist_sellers_dataset.csv")
)
display(sellers_df)

customer_df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Reading the Data from MongoDB**

# COMMAND ----------

import pymongo
# importing module
from pymongo import MongoClient

hostname = "49-f52.h.filess.io"
database = "olistDataNoSQL_meanswetax"
port = "27018"
username = "olistDataNoSQL_meanswetax"
password = "a9f03a9b1de0daed588c6baa134d1745dd39ed77"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase


# COMMAND ----------

import pandas as pd
collection = mydatabase['product_categories']

mongo_data = pd.DataFrame(list(collection.find()))
mongo_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning the Data
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,to_date,datediff,current_date,when


# COMMAND ----------

def clean_dataframe(df,name):
  print("cleaning"+name)
  return df.dropDuplicates().na.drop('all')

orders_df = clean_dataframe(orders_df,'orders')
display(orders_df)

# COMMAND ----------

#COnvert Date Columns

orders_df = orders_df.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp"))) \
    .withColumn("order_approved_at", to_date(col("order_approved_at"))) \
    .withColumn("order_delivered_carrier_date", to_date(col("order_delivered_carrier_date"))) \
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date"))) \
    .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))

display(orders_df)

# COMMAND ----------

#Calculate Delivery and time Delays

orders_df = orders_df.withColumn("actual_delivery_time", datediff("order_delivered_customer_date","order_purchase_timestamp")) \
    .withColumn("estimated_delivery_time", datediff("order_estimated_delivery_date","order_purchase_timestamp")) \
    .withColumn("Delay Time",col("actual_delivery_time") - col("estimated_delivery_time"))

display(orders_df)

# COMMAND ----------

display(orders_df.tail(5))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # **joining multiple Data**

# COMMAND ----------

from pyspark.sql import functions as F

# 0) Make sure join key types match
customer_df  = customer_df.withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast("int"))
sellers_df   = sellers_df.withColumn("seller_zip_code_prefix",   F.col("seller_zip_code_prefix").cast("int"))
geolocation_df = geolocation_df.withColumn("geolocation_zip_code_prefix", F.col("geolocation_zip_code_prefix").cast("int"))

# 1) Collapse geolocation to ONE row per prefix (use first city/state, avg lat/lng)
geo_prefix_df = (
    geolocation_df
    .select("geolocation_zip_code_prefix","geolocation_city","geolocation_state","geolocation_lat","geolocation_lng")
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.first("geolocation_city", ignorenulls=True).alias("geolocation_city"),
        F.first("geolocation_state", ignorenulls=True).alias("geolocation_state"),
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng")
    )
)

# 2) Enrich CUSTOMER dim with geo
customers_enriched = (
    customer_df
    .join(geo_prefix_df, customer_df.customer_zip_code_prefix == geo_prefix_df.geolocation_zip_code_prefix, "left")
    .withColumnRenamed("geolocation_lat",   "customer_lat")
    .withColumnRenamed("geolocation_lng",   "customer_lng")
    .withColumnRenamed("geolocation_city",  "customer_city_geo")
    .withColumnRenamed("geolocation_state", "customer_state_geo")
    .drop("geolocation_zip_code_prefix")
)

# 3) Enrich SELLER dim with geo (rename columns to seller_* first)
geo_prefix_seller = (
    geo_prefix_df
    .withColumnRenamed("geolocation_lat",   "seller_lat")
    .withColumnRenamed("geolocation_lng",   "seller_lng")
    .withColumnRenamed("geolocation_city",  "seller_city_geo")
    .withColumnRenamed("geolocation_state", "seller_state_geo")
)

sellers_enriched = (
    sellers_df
    .join(geo_prefix_seller, sellers_df.seller_zip_code_prefix == geo_prefix_seller.geolocation_zip_code_prefix, "left")
    .drop("geolocation_zip_code_prefix")
)

# 4) Now do the fact-side joins (no blow-up)
orders_customers_df = (
    orders_df
    .join(customers_enriched, orders_df.customer_id == customers_enriched.customer_id, "left")
    .drop(customers_enriched.customer_id)
)

orders_payments_df = (
    orders_customers_df
    .join(payments_df, orders_customers_df.order_id == payments_df.order_id, "left")
    .drop(payments_df.order_id)
)

order_items_df = (
    orders_payments_df
    .join(items_df, "order_id", "left")
    .drop(items_df.order_id)
)

order_items_products_df = (
    order_items_df
    .join(products_df, order_items_df.product_id == products_df.product_id, "left")
    .drop(products_df.product_id)
)

order_reviews_df = (
    order_items_products_df
    .join(reviews_df, order_items_products_df.order_id == reviews_df.order_id, "left")
    .drop(reviews_df.order_id)
)

final_df = (
    order_reviews_df
    .join(sellers_enriched, order_reviews_df.seller_id == sellers_enriched.seller_id, "left")
    .drop(sellers_enriched.seller_id)
)

display(final_df)


# COMMAND ----------

print("orders_df rows:", orders_df.count())
print("customer_df rows:", customer_df.count())
print("geolocation_df rows:", geolocation_df.count())
print("items_df rows:", items_df.count())
print("payments_df rows:", payments_df.count())
print("reviews_df rows:", reviews_df.count())
print("products_df rows:", products_df.count())
print("sellers_df rows:", sellers_df.count())

print("orders_customers_df rows:", orders_customers_df.count())
print("orders_payments_df rows:", orders_payments_df.count())
print("order_items_df rows:", order_items_df.count())
print("order_items_products_df rows:", order_items_products_df.count())
print("order_reviews_df rows:", order_reviews_df.count())
print("final_df rows:", final_df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC orders_customers_df = orders_df.join(customer_df, orders_df.customer_id == customer_df.customer_id, "left").drop(customer_df.customer_id)
# MAGIC print("orders_customers_df rows:", orders_customers_df.count())
# MAGIC
# MAGIC orders_payments_df = orders_customers_df.join(payments_df, orders_customers_df.order_id == payments_df.order_id, "left").drop(payments_df.order_id)
# MAGIC print("orders_payments_df rows:", orders_payments_df.count())
# MAGIC
# MAGIC order_items_df = orders_payments_df.join(items_df, "order_id", "left").drop(items_df.order_id)
# MAGIC print("order_items_df rows:", order_items_df.count())
# MAGIC
# MAGIC order_items_products_df = order_items_df.join(products_df, order_items_df.product_id == products_df.product_id, "left").drop(products_df.product_id)
# MAGIC print("order_items_products_df rows:", order_items_products_df.count())
# MAGIC
# MAGIC order_reviews_df = order_items_products_df.join(reviews_df, order_items_products_df.order_id == reviews_df.order_id, "left").drop(reviews_df.order_id)
# MAGIC print("order_reviews_df rows:", order_reviews_df.count())
# MAGIC
# MAGIC order_sellers_df = order_reviews_df.join(sellers_df, order_reviews_df.seller_id == sellers_df.seller_id, "left").drop(sellers_df.seller_id)
# MAGIC print("order_sellers_df rows:", order_sellers_df.count())
# MAGIC
# MAGIC order_customer_geo_df = (
# MAGIC     order_sellers_df
# MAGIC     .join(
# MAGIC         geolocation_df,
# MAGIC         order_sellers_df.customer_zip_code_prefix == geolocation_df.geolocation_zip_code_prefix,
# MAGIC         "left"
# MAGIC     )
# MAGIC     .withColumnRenamed("geolocation_lat",   "customer_lat")
# MAGIC     .withColumnRenamed("geolocation_lng",   "customer_lng")
# MAGIC     .withColumnRenamed("geolocation_city",  "customer_city_geo")
# MAGIC     .withColumnRenamed("geolocation_state", "customer_state_geo")
# MAGIC     .drop(geolocation_df.geolocation_zip_code_prefix)
# MAGIC )
# MAGIC print("order_customer_geo_df rows:", order_customer_geo_df.count())
# MAGIC
# MAGIC final_df = (
# MAGIC     order_customer_geo_df
# MAGIC     .join(
# MAGIC         geolocation_df,
# MAGIC         order_customer_geo_df.seller_zip_code_prefix == geolocation_df.geolocation_zip_code_prefix,
# MAGIC         "left"
# MAGIC     )
# MAGIC     .withColumnRenamed("geolocation_lat",   "seller_lat")
# MAGIC     .withColumnRenamed("geolocation_lng",   "seller_lng")
# MAGIC     .withColumnRenamed("geolocation_city",  "seller_city_geo")
# MAGIC     .withColumnRenamed("geolocation_state", "seller_state_geo")
# MAGIC     .drop(geolocation_df.geolocation_zip_code_prefix)
# MAGIC )
# MAGIC print("final_df rows:", final_df.count())

# COMMAND ----------

display(final_df)

# COMMAND ----------

mongo_data.drop('_id',axis=1,inplace=True)
mongo_spark_df = spark.createDataFrame(mongo_data)
display(mongo_spark_df)

# COMMAND ----------

final_df = final_df.join(mongo_spark_df,"product_category_name","left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

print("final_df rows:", final_df.count())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/silver")

# COMMAND ----------

final_df = spark.read.parquet("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/silver")

# COMMAND ----------

display(final_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col
import pandas as pd
missing_counts = {col_name: final_df.filter(col(col_name).isNull()).count() for col_name in final_df.columns}
display(pd.DataFrame(list(missing_counts.items()), columns=["column", "missing_count"]))

# COMMAND ----------

from pyspark.sql.functions import col, lower, current_date

# Save final_df to another name for cleaning
cleaned_df = final_df

# Drop rows where critical columns are missing (example: order_id, customer_id, product_id)
critical_cols = ["order_id", "customer_id", "product_id"]
cleaned_df = cleaned_df.na.drop(subset=critical_cols)

# Fill missing numeric columns with 0 (example: payment_value, price, freight_value)
fill_zero_cols = ["payment_value", "price", "freight_value"]
cleaned_df = cleaned_df.na.fill({col: 0 for col in fill_zero_cols})

# Fill missing string columns with 'unknown' (example: product_category_name, customer_city_geo)
fill_unknown_cols = ["product_category_name", "customer_city_geo", "customer_state_geo", "seller_city_geo", "seller_state_geo"]
cleaned_df = cleaned_df.na.fill({col: "unknown" for col in fill_unknown_cols})

# Standardize string columns to lowercase (example: product_category_name)
cleaned_df = cleaned_df.withColumn("product_category_name", lower(col("product_category_name")))

# Change a date column to current date (example: order_purchase_timestamp)
if "order_purchase_timestamp" in cleaned_df.columns:
    cleaned_df = cleaned_df.withColumn("order_purchase_timestamp", current_date())

# Remove duplicates
cleaned_df = cleaned_df.distinct()

# Display cleaned data
display(cleaned_df)

# Other cleaning suggestions:
# Remove outliers in numeric columns (e.g., price, payment_value)
max_price = 10000
max_payment_value = 10000
cleaned_df = cleaned_df.filter(
    (col("price") >= 0) & (col("price") <= max_price) &
    (col("payment_value") >= 0) & (col("payment_value") <= max_payment_value)
)

# Standardize date formats if needed (e.g., order_approved_at to 'yyyy-MM-dd')
from pyspark.sql.functions import to_date
if "order_approved_at" in cleaned_df.columns:
    cleaned_df = cleaned_df.withColumn("order_approved_at", to_date(col("order_approved_at")))

# Remove rows with inconsistent or impossible values (e.g., negative prices)
if "freight_value" in cleaned_df.columns:
    cleaned_df = cleaned_df.filter(col("freight_value") >= 0)

# COMMAND ----------

print("cleaned_df rows:", cleaned_df.count())

# COMMAND ----------

cleaned_df.write.mode("overwrite").parquet("abfss://olistdata@shaolistdatastorage.dfs.core.windows.net/silver/cleaned")