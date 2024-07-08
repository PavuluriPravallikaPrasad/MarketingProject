# Databricks notebook source
 # dbutils.fs.mount(
#   source = 'wasbs://input-data@ministorageaccount.blob.core.windows.net/',
#   mount_point = '/mnt/transforming_data',
#   extra_configs = {'fs.azure.account.key.ministorageaccount.blob.core.windows.net':'RAbA5q7dAljD2wIK34CIJXO2hQfpTTCovXCn3meILRD2wCImLwboZAGa9kiHdtl1ML/rWH4KYmpZ+AStok9LhA=='}
# )

# COMMAND ----------

file_path = 'dbfs:/mnt/transforming_data/PersonalDetails.csv'
df = spark.read.csv(file_path,header=True, inferSchema=True)
df.write.format('delta').saveAsTable('databricksworkspace1.default.Customer_PersonalDetails')
display(spark.sql("select * from databricksworkspace1.default.Customer_PersonalDetails"))

# COMMAND ----------

file_path = 'dbfs:/mnt/transforming_data/PurchaseDetails.csv'
df1 = spark.read.csv(file_path,header=True, inferSchema=True)
df1.write.format('delta').saveAsTable('databricksworkspace1.default.Customer_PurchaseDetails')
display(spark.sql("select * from databricksworkspace1.default.Customer_PurchaseDetails"))

# COMMAND ----------

df.printSchema()
df1.printSchema()

# COMMAND ----------

#3rd

from pyspark.sql.functions import *

df = df.withColumn('DOB',concat(col('DOB').cast('String'),lit('/07/07')))
df = df.withColumn('DOB', when(df.CustomerID == 2174,'1998/04/07').when(df.Education == 'Basic', '1972/03/20').otherwise(df.DOB))

# edu_count = df4.groupBy('Education').count()
# df4 = df4.withColumn(col('Education') < 60, lit("03/20/1972").otherwise(df4.DOB))

display(df)

# COMMAND ----------

display(df.filter(col("Education") == 'Basic').select("DOB"))

# COMMAND ----------

#3rd

# df = df.withColumn('DOB',col('DOB').cast('String'))
# df.printSchema()
# df = df.withColumn('DOB',col('DOB').cast('date'))
# df.printSchema()
df = df.withColumn("DOB", to_date(col("DOB"), "yyyy/MM/dd"))
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.select('CustomerID').distinct())

# COMMAND ----------

df = df.orderBy(asc('CustomerID'))
display(df)

# COMMAND ----------

#4th

df = df.withColumn('CustomerID', when(col('CustomerID')== 1,3).otherwise(df.CustomerID));
df = df.withColumn('CustomerID', when(col('CustomerID')== 0,1).otherwise(df.CustomerID));
display(df)

# COMMAND ----------

#7th

from pyspark.sql.functions import *
df = df.withColumn('Kidhome', when((col('Marital_Status') == 'Married') & (col('Kidhome') >= 1) & (col('Income') < 26000), 3).otherwise(col('Kidhome')));
df = df.withColumn('Teenhome', when((col('Marital_Status') == 'Married') & (col('Kidhome') >= 1) & (col('Income') < 26000), 2).otherwise(col('Teenhome')));
display(df)


# COMMAND ----------


display(df.filter((col('Marital_Status') == 'Married') & (col('Income') < 30000) & (col('Kidhome') > 1) & (col('Teenhome') > 1)).select('Marital_Status', 'Income', 'Kidhome', 'Teenhome'))



# COMMAND ----------

#---------------------------df1------------------------------------

# COMMAND ----------

df1 = df1.orderBy(asc('CustomerID'))
display(df1)

# COMMAND ----------

#4th

df1 = df1.withColumn('CustomerID', when(col('CustomerID')== 1,3).otherwise(df1.CustomerID));
df1 = df1.withColumn('CustomerID', when(col('CustomerID')== 0,1).otherwise(df1.CustomerID));
display(df1)

# COMMAND ----------

#5th

df1 = df1.withColumn('RewardPoints_Wine', when(col('MntWines') >= 30, floor(col('MntWines') / 30)).otherwise(0));
df1 = df1.withColumn('RewardPoints_Meat', when((col('MntMeatProducts') + col('MntFishProducts')) >= 50, floor((col('MntMeatProducts') + col('MntFishProducts'))/50)).otherwise(0));
df1 = df1.withColumn('RewardPoints_Sweets', when(col('MntSweetProducts') >= 20, ceil(col('MntSweetProducts')/20)).otherwise(0));
df1 = df1.withColumn('RewardPoints_Gold', when(col('MntGoldProds') >= 100, floor(col('MntGoldProds')/100)).otherwise(0));
display(df1)


# COMMAND ----------

#6th

from pyspark.sql import *
df1 = df1.withColumn('CashBack_Earned', when((col('MntWines') >= 30) & (col('MntMeatProducts') + col('MntFishProducts') >= 50) & (col('MntSweetProducts') >= 20), floor((col('MntWines') + col('MntMeatProducts') + col('MntFishProducts') + col('MntSweetProducts')) * 0.1)).otherwise(0))
display(df1)

# COMMAND ----------

df1 = df1.withColumn('Customer_Type', when(col('RewardPoints_Gold') >= 2, "Gold Buyers").when(col('RewardPoints_Sweets') >= 4, "Sweet Tooth").when(col('RewardPoints_Meat') >= 5, 'Food Lover').when((col('RewardPoints_Wine') >= 5) & (col('RewardPoints_Wine') < 10), "White Wine lover").when(col('RewardPoints_Wine') >= 10, "Red Wine lover").otherwise('General Customer'))
# display(df1)
df1.printSchema()

# COMMAND ----------

# from pyspark.sql.functions import *;

# df1_var = df1.select('Recency')
# recency_values = [row.Recency for row in df1_var.collect()]
# for val in recency_values:
#     if val < 90:
#         df9.withColumn('CashbackStatus', lit('Expired'))
#     else:
#         df9.withColumn('CashbackStatus', lit('Active'))

# # display(df7)

# COMMAND ----------

display(df)

# COMMAND ----------

# null_val_df = df.filter(col("CustomerID").isNull() | col("DOB").isNull() | col("Education").isNull() | col("Marital_Status").isNull() | col("Income").isNull() | col("Kidhome").isNull() | col("Teenhome").isNull())
# display(null_val_df)

# COMMAND ----------

display(df1)

# COMMAND ----------

null_val_df1 = df1.filter(col("CustomerID").isNull() | col("Dt_Customer").isNull() | col("Recency").isNull() | col("MntWines").isNull() | col("MntFruits").isNull() | col("MntMeatProducts").isNull() | col("MntFishProducts").isNull() | col("MntSweetProducts").isNull()| col("MntGoldProds").isNull())
display(null_val_df1)

# COMMAND ----------

col_to_fillna = ['Income']
df = df.fillna(0, subset=col_to_fillna)
display(df)

# COMMAND ----------

df.createOrReplaceGlobalTempView('PersonalDetails_GTV')

# COMMAND ----------

df1.createOrReplaceGlobalTempView('PurchaseDetails_GTV')

# COMMAND ----------

display(spark.sql("select * from global_temp.PersonalDetails_GTV"));
display(spark.sql("select * from global_temp.PurchaseDetails_GTV"));
