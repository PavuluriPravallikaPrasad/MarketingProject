# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.ministorageaccount.dfs.core.windows.net",
    "RAbA5q7dAljD2wIK34CIJXO2hQfpTTCovXCn3meILRD2wCImLwboZAGa9kiHdtl1ML/rWH4KYmpZ+AStok9LhA=="
)

table_path = "abfss://curated-data@ministorageaccount.dfs.core.windows.net/processing/"
spark.sql("create table final_marketing_table using DELTA location 'abfss://curated-data@ministorageaccount.dfs.core.windows.net/processing/' as select * from databricksworkspace1.default.Final_Marketing_Table")


# COMMAND ----------

display(spark.read.format("delta").load(table_path))
