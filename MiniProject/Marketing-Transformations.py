# Databricks notebook source
# DBTITLE 1,Creating the DF from the mounted path and creating a table in default database.
file_path = 'dbfs:/mnt/transforming_data/PurchaseStatistics.csv'
df3 = spark.read.csv(file_path, header=True, inferSchema=True)
df3.write.format('delta').saveAsTable('databricksworkspace1.default.Customer_PurchaseStatistics')
display(spark.sql("select * from databricksworkspace1.default.Customer_PurchaseStatistics"))

# COMMAND ----------

# DBTITLE 1,Changing the customerID values from 0 to 1 and 1 to 3 of df3.
from pyspark.sql.functions import *
df3 = df3.withColumn('CustomerID', when(col('CustomerID')== 1,3).otherwise(df3.CustomerID));
df3 = df3.withColumn('CustomerID', when(col('CustomerID')== 0,1).otherwise(df3.CustomerID));
display(df3)

# COMMAND ----------

# DBTITLE 1,Calculating the average recency using aggregate function.
from pyspark.sql.functions import *
Avg_Recenct = df3.agg(avg('Recency').alias('Average Recency'))
display(Avg_Recenct)

# COMMAND ----------

# DBTITLE 1,Displaying the values of the table.
display(spark.sql("select * from databricksworkspace1.default.Customer_PurchaseStatistics"))

# COMMAND ----------

# DBTITLE 1,Changing the customerID values from 0 to 1 and 1 to 3 in the table.
spark.sql("update databricksworkspace1.default.Customer_PurchaseStatistics set CustomerID = 3 where CustomerID = 1")
spark.sql("update databricksworkspace1.default.Customer_PurchaseStatistics set CustomerID = 1 where CustomerID = 0")

# COMMAND ----------

# DBTITLE 1,Adding column: Customer_Stats.
from pyspark.sql.functions import *


df3 = df3.withColumn('Customer_Stats',when((col('NumDealsPurchases') > 10) & (col('NumWebPurchases') > 5), 'Frequent Buyers').when(col('NumWebPurchases') > 10 , 'Web Shoppers').when(col('NumCatalogPurchases') > 10, 'Catalog Enthusiasts').otherwise('Other')) 
display(df3)


# COMMAND ----------

# DBTITLE 1,Adding Column: TotalSpending.
df3 = df3.withColumn('TotalSpending', col('NumDealsPurchases') + col('NumWebPurchases') + col('NumCatalogPurchases') + col('NumStorePurchases'))
df3 = df3.withColumn('AvgSpending', col('TotalSpending')/4)
display(df3)

# COMMAND ----------

# DBTITLE 1,Printing the Schema for df3.
df3.printSchema()

# COMMAND ----------

# DBTITLE 1,Adding Column: CustomerLTV
# https://blog.hubspot.com/service/how-to-calculate-customer-lifetime-value#how-to-calc-cltv

df3 = df3.withColumn('CustomerLTV', col('TotalSpending') * col('AvgSpending'))
display(df3)

# COMMAND ----------

# DBTITLE 1,Extracting the month and year from the Dt_Customer column.
from pyspark.sql.functions import *
df3 = df3.withColumn('Month', month(col('Dt_Customer'))).withColumn('Year', year(col('Dt_Customer')))
display(df3)

# COMMAND ----------

# DBTITLE 1,Creating a dataframe month_stat using aggregate function sum on the corresponding columns.
# To identify sale season based on different mode of purchases

month_stat = df3.groupBy('CustomerID','Year','Month').agg(sum('NumDealsPurchases').alias('TotalDealSales'),sum('NumWebPurchases').alias('TotalWebSales'),sum('NumCatalogPurchases').alias('TotalCatalogSales'),sum('NumStorePurchases').alias('TotalInStoreSales')).orderBy('Year','Month')
display(month_stat)

# COMMAND ----------

display(df3)

# COMMAND ----------

# DBTITLE 1,Checking for the null values.
null_val_df3 = df3.filter(col("CustomerID").isNull() | col("Dt_Customer").isNull() | col("Recency").isNull() | col("NumDealsPurchases").isNull() | col("NumWebPurchases").isNull() | col("NumCatalogPurchases").isNull() | col("NumStorePurchases").isNull() | col("NumWebVisitsMonth").isNull())
display(null_val_df3)

# COMMAND ----------

# DBTITLE 1,Creating the dataframes to Global tempViews.
df3.createOrReplaceGlobalTempView('PurchaseStats_GTV')
month_stat.createOrReplaceGlobalTempView('Month_stats_GTV')

# COMMAND ----------

display(spark.sql("select * from global_temp.PurchaseStats_GTV"))
display(spark.sql("select * from global_temp.Month_stats_GTV"))
