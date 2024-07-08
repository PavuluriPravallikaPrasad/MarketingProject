# Databricks notebook source
file_path = 'dbfs:/mnt/transforming_data/PurchaseStatistics.csv'
df3 = spark.read.csv(file_path, header=True, inferSchema=True)
df3.write.format('delta').saveAsTable('databricksworkspace1.default.Customer_PurchaseStatistics')
display(spark.sql("select * from databricksworkspace1.default.Customer_PurchaseStatistics"))

# COMMAND ----------

from pyspark.sql.functions import *
df3 = df3.withColumn('CustomerID', when(col('CustomerID')== 1,3).otherwise(df3.CustomerID));
df3 = df3.withColumn('CustomerID', when(col('CustomerID')== 0,1).otherwise(df3.CustomerID));
display(df3)

# COMMAND ----------

from pyspark.sql.functions import *
Avg_Recenct = df3.agg(avg('Recency').alias('Average Recency'))
display(Avg_Recenct)

# COMMAND ----------

display(spark.sql("select * from databricksworkspace1.default.Customer_PurchaseStatistics"))

# COMMAND ----------

spark.sql("update databricksworkspace1.default.Customer_PurchaseStatistics set CustomerID = 3 where CustomerID = 1")
spark.sql("update databricksworkspace1.default.Customer_PurchaseStatistics set CustomerID = 1 where CustomerID = 0")

# COMMAND ----------

from pyspark.sql.functions import *


df3 = df3.withColumn('Customer_Stats',when((col('NumDealsPurchases') > 10) & (col('NumWebPurchases') > 5), 'Frequent Buyers').when(col('NumWebPurchases') > 10 , 'Web Shoppers').when(col('NumCatalogPurchases') > 10, 'Catalog Enthusiasts').otherwise('Other')) 
display(df3)


# COMMAND ----------

df3 = df3.withColumn('TotalSpending', col('NumDealsPurchases') + col('NumWebPurchases') + col('NumCatalogPurchases') + col('NumStorePurchases'))
df3 = df3.withColumn('AvgSpending', col('TotalSpending')/4)
display(df3)

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

# https://blog.hubspot.com/service/how-to-calculate-customer-lifetime-value#how-to-calc-cltv

df3 = df3.withColumn('CustomerLTV', col('TotalSpending') * col('AvgSpending'))
display(df3)

# COMMAND ----------

from pyspark.sql.functions import *
df3 = df3.withColumn('Month', month(col('Dt_Customer'))).withColumn('Year', year(col('Dt_Customer')))
display(df3)

# COMMAND ----------

# To identify sale season based on different mode of purchases

month_stat = df3.groupBy('CustomerID','Year','Month').agg(sum('NumDealsPurchases').alias('TotalDealSales'),sum('NumWebPurchases').alias('TotalWebSales'),sum('NumCatalogPurchases').alias('TotalCatalogSales'),sum('NumStorePurchases').alias('TotalInStoreSales')).orderBy('Year','Month')
display(month_stat)

# COMMAND ----------

display(df3)

# COMMAND ----------

null_val_df3 = df3.filter(col("CustomerID").isNull() | col("Dt_Customer").isNull() | col("Recency").isNull() | col("NumDealsPurchases").isNull() | col("NumWebPurchases").isNull() | col("NumCatalogPurchases").isNull() | col("NumStorePurchases").isNull() | col("NumWebVisitsMonth").isNull())
display(null_val_df3)

# COMMAND ----------

df3.createOrReplaceGlobalTempView('PurchaseStats_GTV')
month_stat.createOrReplaceGlobalTempView('Month_stats_GTV')

# COMMAND ----------

display(spark.sql("select * from global_temp.PurchaseStats_GTV"))
display(spark.sql("select * from global_temp.Month_stats_GTV"))