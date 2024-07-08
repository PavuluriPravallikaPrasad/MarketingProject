# Databricks notebook source
# DBTITLE 1,Details based on CustomerID
ref_CustID = spark.sql("select * from databricksworkspace1.default.Final_Marketing_Table where CustomerID between 1 and 1500").collect()
# dis_CustomerID = wid_df1.select('CustomerID').distinct().orderBy('CustomerID').collect()
fin_CustomerID = [str(var.CustomerID) for var in ref_CustID]
dbutils.widgets.dropdown("Customer_ID", fin_CustomerID[0], fin_CustomerID, "Customer Identification")
res_CID = dbutils.widgets.get("Customer_ID")
# print(res_CID)
output = spark.sql(f"select * from databricksworkspace1.default.Final_Marketing_Table where CustomerID='{res_CID}'")
display(output)

# COMMAND ----------

# DBTITLE 1,Creating Widgets and Retrieving the values
ref = spark.sql("select * from databricksworkspace1.default.Final_Marketing_Table where CustomerID between 1 and 1500").collect()
# dis_CustomerID = wid_df1.select('CustomerID').distinct().orderBy('CustomerID').collect()
fin_CustomerID = [str(var.CustomerID) for var in ref]
fin_Income = [str(var2.Income) for var2 in ref]
dbutils.widgets.dropdown("Customer_ID", fin_CustomerID[0], fin_CustomerID, "Customer Identification")
dbutils.widgets.dropdown("Customer_Income", fin_Income[0], fin_Income,"Customer Income Details")
res_Cid = dbutils.widgets.get("Customer_ID")
res_Cincome = dbutils.widgets.get("Customer_Income")


# COMMAND ----------

# DBTITLE 1,Based on Income and CustomerID
ref_df = spark.createDataFrame(ref)
filter_df = ref_df.filter((ref_df.CustomerID == res_Cid) & (ref_df.Income == res_Cincome))
if filter_df.count() > 0:
    result = spark.sql(f"select * from global_temp.PersonalDetails_GTV where CustomerID = '{res_Cid}' and Income = '{res_Cincome}'")
    display(result)
else:
    display("Donot exist")
# display(ref)

# COMMAND ----------

# DBTITLE 1,Income and Reward Points - wine
ref = spark.sql("select * from databricksworkspace1.default.Final_Marketing_Table where CustomerID between 1 and 1500").collect()
ref_df = spark.createDataFrame(ref)
fin_Income = [str(var.Income) for var in ref]
fin_RewardPoints_Wine = [str(var.RewardPoints_Wine) for var in ref]
dbutils.widgets.dropdown("Customer_Income", fin_Income[0], fin_Income, "Customer Income")
dbutils.widgets.dropdown("Customer_RewardPoints_Wine", fin_RewardPoints_Wine[0], fin_RewardPoints_Wine, "Customer Wine Reward Points")
res_Income = dbutils.widgets.get("Customer_Income")
res_RewardPoints_Wine = dbutils.widgets.get("Customer_RewardPoints_Wine")
filtered_df = ref_df.filter((ref_df.Income == res_Income)&(ref_df.RewardPoints_Wine == res_RewardPoints_Wine))
if filtered_df.count() > 0:
    result = spark.sql(f"select * from databricksworkspace1.default.Final_Marketing_Table where Income = '{res_Income}' and RewardPoints_Wine = '{res_RewardPoints_Wine}'")
    display(result)
else:
    display("Do not exist")



# COMMAND ----------

# DBTITLE 1,Wine, Meat,Sweet Products
# ref = spark.sql("select * from databricksworkspace1.default.Final_Marketing_Table where CustomerID between 1 and 1500").collect()
# fin_WinePurchases = list([var.MntWines for var in ref])
# fin_MeatPurchases = list([var.MntMeatProducts for var in ref])
# fin_SweetPurchases = list([var.MntSweetProducts for var in ref])
# dbutils.widgets.dropdown("Wine_Purchases", fin_WinePurchases[0], fin_WinePurchases, "Number of Wine Purchases")
# dbutils.widgets.dropdown("Meat_Purchases", fin_MeatPurchases[0], fin_MeatPurchases, "Number of Meat Purchases")
# dbutils.widgets.dropdown("Sweet_Purchases", fin_SweetPurchases[0], fin_SweetPurchases, "Number of Sweet Purchases")
# res_WinePurchases = dbutils.widgets.get("Wine_Purchases")
# res_MeatPurchases = dbutils.widgets.get("Meat_Purchases")
# res_SweetPurchases = dbutils.widgets.get("Sweet_Purchases")
# ref_df = spark.createDataFrame(ref)
# filter_df = ref_df.filter((ref_df.MntWines == res_WinePurchases)&(ref_df.MntMeatProducts == res_MeatPurchases)&(ref_df.MntSweetProducts == res_SweetPurchases))
# if filter_df.count() > 0:
#     result = spark.sql(f"select * from databricksworkspace1.default.Final_Marketing_Table where MntWines = '{res_WinePurchases}' and MntMeatProducts = '{res_MeatPurchases}' and MntSweetProducts = '{res_SweetPurchases}'")
#     display(result)
# else:
#     display("Donot exist")



# COMMAND ----------

# DBTITLE 1,Recency and Discounts
dbutils.widgets.text("Customer_Recency", "", "Customer Recency")
dbutils.widgets.text("Customer_Discounts", "", "Customer Discounts")
res_Recency = dbutils.widgets.get("Customer_Recency")
res_Discounts = dbutils.widgets.get("Customer_Discounts")
ref_df = spark.sql("select * from databricksworkspace1.default.Final_Marketing_Table")
filter_df = ref_df.filter((ref_df.Recency < res_Recency) &(ref_df.Discounts >= res_Discounts))
if filter_df.count() > 0:
    display(filter_df)
else:
    display("Do not exist")


# COMMAND ----------

# DBTITLE 1,Filter by Buyer Category, Reward Points, and Discounts
dbutils.widgets.dropdown("Customer_Buyer_Ctg", "Occasional Customer - No Discounts", ["Occasional Customer - No Discounts", "Premium Wine Buyer", "Frequent Sweets Buyer", ""], "Buyer Category")
dbutils.widgets.text("Customer_RewardPoints", "", "Customer Total Reward Points")
dbutils.widgets.text("Customer_Discounts", "", "Customer Discounts")
res_Buyer_Ctg = dbutils.widgets.get("Customer_Buyer_Ctg")
res_RewardPoints = dbutils.widgets.get("Customer_RewardPoints")
res_Discounts = dbutils.widgets.get("Customer_Discounts")
ref_df = spark.sql("select *, (RewardPoints_Wine + RewardPoints_Meat + RewardPoints_Sweets + RewardPoints_Gold) AS TotalRewardPoints from databricksworkspace1.default.Final_Marketing_Table")
filter_df = ref_df.filter((ref_df.Buyer_Ctg == res_Buyer_Ctg) &(ref_df.TotalRewardPoints >= res_RewardPoints) &(ref_df.Discounts > res_Discounts))
if filter_df.count() > 0:
    display(filter_df)
else:
    display("Do not exist")


# COMMAND ----------

# DBTITLE 1,Filter by Average Income and Total Spending
from pyspark.sql.functions import *
dbutils.widgets.text("Customer_AvgIncome", "", "Customer Average Income")
dbutils.widgets.text("Customer_TotalSpending", "", "Customer Total Spending")
res_AvgIncome = dbutils.widgets.get("Customer_AvgIncome")
res_TotalSpending = dbutils.widgets.get("Customer_TotalSpending")
ref_df = spark.sql("select * from databricksworkspace1.default.Final_Marketing_Table")
agg_df = ref_df.groupBy("CustomerID").agg(avg("Income").alias("AvgIncome"),sum(col("MntWines") + col("MntMeatProducts") + col("MntFishProducts") + col("MntSweetProducts") + col("MntGoldProds")).alias("TotalSpending"))
filter_df = agg_df.filter((agg_df.AvgIncome > res_AvgIncome)&(agg_df.TotalSpending > res_TotalSpending))
if filter_df.count() > 0:
    display(filter_df)
else:
    display("Do not exist")


# COMMAND ----------

from pyspark.sql.functions import *
dbutils.widgets.text("Customer_MaxWinesSpending", "", "Customer Max Wine Spend")
dbutils.widgets.text("Customer_AvgMeatsSpending", "", "Customer Avg Meat Spend")
res_MaxWinesSpending = dbutils.widgets.get("Customer_MaxWinesSpending")
res_AvgMeatsSpending = dbutils.widgets.get("Customer_AvgMeatsSpending")
ref_df = spark.sql("select * from databricksworkspace1.default.Final_Marketing_Table")
agg_df = ref_df.groupBy("CustomerID").agg(max("MntWines").alias("MaxWinesSpending"),avg("MntMeatProducts").alias("AvgMeatsSpending"))
filter_df = agg_df.filter((agg_df.MaxWinesSpending > res_MaxWinesSpending) &(agg_df.AvgMeatsSpending > res_AvgMeatsSpending))

if filter_df.count() > 0:
    display(filter_df)
else:
    display("Donot exist")

