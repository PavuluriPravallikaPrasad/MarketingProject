# Databricks notebook source
# DBTITLE 1,Storing the table to ADLS.
spark.conf.set(
    "fs.azure.account.key.ministorageaccount.dfs.core.windows.net",
    "RAbA5q7dAljD2wIK34CIJXO2hQfpTTCovXCn3meILRD2wCImLwboZAGa9kiHdtl1ML/rWH4KYmpZ+AStok9LhA=="
)
spark.sql("create table final_CustomerMarketing_table using DELTA location 'abfss://curated-data@ministorageaccount.dfs.core.windows.net/processing/' as select * from databricksworkspace1.default.Final_Marketing_Table")


# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.ministorageaccount.dfs.core.windows.net",
    "RAbA5q7dAljD2wIK34CIJXO2hQfpTTCovXCn3meILRD2wCImLwboZAGa9kiHdtl1ML/rWH4KYmpZ+AStok9LhA=="
)
table_path = "abfss://curated-data@ministorageaccount.dfs.core.windows.net/processing"
display(spark.read.format("delta").load(table_path))

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.ministorageaccount.dfs.core.windows.net",
    "RAbA5q7dAljD2wIK34CIJXO2hQfpTTCovXCn3meILRD2wCImLwboZAGa9kiHdtl1ML/rWH4KYmpZ+AStok9LhA=="
)
spark.sql("drop table if exists final_CustomerMarketing_table")

