# Databricks notebook source
# DBTITLE 1,Displaying the contents of all the global tempviews.
display(spark.sql("select * from global_temp.PersonalDetails_GTV"));
display(spark.sql("select * from global_temp.PurchaseDetails_GTV"));
display(spark.sql("select * from global_temp.PurchaseStats_GTV"))
display(spark.sql("select * from global_temp.Month_stats_GTV"))

# COMMAND ----------

display(spark.sql("select PerD.CustomerID, PerD.DOB,PurD.Dt_Customer, PerD.Marital_Status, PerD.Income, PerD.Kidhome, PerD.Teenhome,PurD.Recency, PurD.MntWines,PurD.MntMeatProducts, PurD.MntFishProducts, PurD.MntSweetProducts, PurD.MntGoldProds, PurD.RewardPoints_Wine, PurD.RewardPoints_Meat, PurD.RewardPoints_Sweets, PurD.RewardPoints_Gold, PurD.CashBack_Earned, PurD.Customer_Type  from global_temp.PersonalDetails_GTV as PerD join global_temp.PurchaseDetails_GTV as PurD using (CustomerID)"))

# COMMAND ----------

# DBTITLE 1,Performing natural join on PerD with PurD based on CustomerID.
# To store the result into the DB :-
# Perform Joins -> Convert to Dataframe -> Save the Dataframe to DB.
# To perform other joins, again convert the Dataframe to TempView.

join_df = spark.sql("select PerD.CustomerID, PerD.DOB,PurD.Dt_Customer, PerD.Marital_Status, PerD.Income, PerD.Kidhome, PerD.Teenhome,PurD.Recency, PurD.MntWines,PurD.MntMeatProducts, PurD.MntFishProducts, PurD.MntSweetProducts, PurD.MntGoldProds, PurD.RewardPoints_Wine, PurD.RewardPoints_Meat, PurD.RewardPoints_Sweets, PurD.RewardPoints_Gold, PurD.CashBack_Earned, PurD.Customer_Type  from global_temp.PersonalDetails_GTV as PerD join global_temp.PurchaseDetails_GTV as PurD using (CustomerID)")

display(join_df)

join_df.write.format("delta").saveAsTable('databricksworkspace1.default.Personal_Purchase_Details')
display(spark.sql("select * from databricksworkspace1.default.Personal_Purchase_Details"))

# COMMAND ----------

# DBTITLE 1,Performing left outer join on MonthSt with PurSt based on CustomerID.
# Performing Left Join on Purchase Statistics with corresponding to Month Stat, where if the Store Purchases > Web Purchases, printing the CustomerID, Dt_Customer,... from Purchase Stats and TotalWebSales from Month Stats (only that corresponds the CustomerID, that satisfies the where condition)

Pur_Stat_df1 = spark.sql("select PurSt.CustomerID, PurSt.Dt_Customer, PurSt.NumDealsPurchases, PurSt.NumWebPurchases, PurSt.NumStorePurchases, PurSt.NumWebVisitsMonth, PurSt.Customer_Stats, PurSt.TotalSpending, PurSt.AvgSpending, PurSt.CustomerLTV, MonthSt.* from global_temp.PurchaseStats_GTV as PurSt left outer join global_temp.Month_stats_GTV as MonthSt using (CustomerID) where (PurSt.NumStorePurchases > PurSt.NumWebPurchases)")
display(Pur_Stat_df1)


# COMMAND ----------

# DBTITLE 1,Performing Left join on PurSt with Month when Numwebpurchases > NumwibvisitsMonth
# Converting the above created Dataframe to a tempView to perform futher trans.

Pur_Stat_df1.createOrReplaceGlobalTempView('PurStat_df1_GTV')

# Performing Left Join on PurStat_df1_GTV with corresponding to Month_Stat_GTV, where if the Web Purchases > Web Visits, printing the rows from PurStat_df1_GTV and Month_Stat_GTV (only that corresponds the CustomerID, that satisfies the where condition)
#----------------------------------------
# display(spark.sql("select PurSt1.Dt_Customer, PurSt1.NumDealsPurchases, PurSt1.NumWebPurchases, PurSt1.NumWebVisitsMonth, PurSt1.NumStorePurchases, PurSt1.Customer_Stats, PurSt1.TotalSpending, PurSt1.AvgSpending, PurSt1.CustomerLTV, MonthSt.Year, MonthSt.Month, MonthSt.TotalDealSales, MonthSt.TotalWebSales, MonthSt.TotalCatalogSales, MonthSt.TotalInStoreSales from global_temp.PurStat_df1_GTV as PurSt1 left outer join global_temp.Month_stats_GTV as MonthSt using (CustomerID) where (PurSt1.NumWebPurchases > PurSt1.NumWebVisitsMonth)"))
#----------------------------------------
Pur_Stat_df2 = spark.sql("select PurSt1.*, MonthSt.Year, MonthSt.Month, MonthSt.TotalDealSales, MonthSt.TotalWebSales, MonthSt.TotalCatalogSales, MonthSt.TotalInStoreSales from global_temp.PurStat_df1_GTV as PurSt1 left outer join global_temp.Month_stats_GTV as MonthSt using (CustomerID) where (PurSt1.NumWebPurchases > PurSt1.NumWebVisitsMonth)")

display(Pur_Stat_df2)

Pur_Stat_df2.createOrReplaceGlobalTempView('Pur_Stat_df2_GTV')


# COMMAND ----------

# DBTITLE 1,Performing right join on PurSt with Month using CustomerID, when NumDealPurchases >= 5 and NumStorePurchases > 10.
# Performing Left Join on Pur_Stat_df2_GTV with corresponding to Month_Stat_GTV, where if NumDealsPurchases >=5 and NumStorePurchases > 10, printing the rows from PurStat_df1_GTV and Month_Stat_GTV (to analyse the Total sales on different modes of purchase) (Only for those customers where )

Pur_Stat_df3 = spark.sql("select PurSt1.Dt_Customer, MonthSt.* from global_temp.Pur_Stat_df2_GTV as PurSt1 right outer join global_temp.Month_stats_GTV as MonthSt using (CustomerID) where (PurSt1.NumDealsPurchases >=5 and PurSt1.NumStorePurchases > 10)")

display(Pur_Stat_df3)

# COMMAND ----------

display(spark.sql("select * from global_temp.PersonalDetails_GTV"))

# COMMAND ----------

display(spark.sql("select * from global_temp.PurchaseStats_GTV"))

# COMMAND ----------

# DBTITLE 1,Performing right join on PurSt with PerD, when marital_Status = married and Customer_Stats = Frequent Buyer and AvgSPending > avg of all the average spendings.
# Customer marital status = married, num of deal purchases and customer type along with all personal details.

# spark.sql("select PurS.*,PerD.* from global_temp.PurchaseStats_GTV as PurS right outer join global_temp.PersonalDetails_GTV as PerD using (CustomerID) where PerD.Marital_Status = 'Married' and PurS.Customer_Stats = 'Frequent Buyers' and PurS.AvgSpending = (select avg(PurS.AvgSpending) from global_temp.PurchaseStats_GTV)")

RightJoin_df2 = spark.sql("select PerD.*, PurS.NumDealsPurchases, PurS.Customer_Stats, PurS.AvgSpending from global_temp.PurchaseStats_GTV as PurS right outer join global_temp.PersonalDetails_GTV as PerD on PurS.CustomerID = PerD.CustomerID where PerD.Marital_Status = 'Married' and PurS.Customer_Stats = 'Frequent Buyers' and PurS.AvgSpending > (select avg(PurS1.AvgSpending) from global_temp.PurchaseStats_GTV as PurS1)")

display(RightJoin_df2)


# COMMAND ----------

# DBTITLE 1,Performing left join on PerD with PurSt using CustomerID when Recency >= 30 and Income > 30000
# Inner Join on PurD with PerD using CustomerID. 
# Left Join PurD, PerD, PurSt -> Performing left join Innerjoin and PurSt. Checks for all records from inner join and matching records from PurSt, where CustomerID and Dt_Customer must match and Recency in PurSt > 30.


display(spark.sql("select PurD.Dt_Customer, PurD.Recency, PurD.CashBack_Earned, PerD.Income,PerD.Education, PurSt.* from global_temp.PurchaseDetails_GTV PurD inner join global_temp.PersonalDetails_GTV PerD left outer join global_temp.PurchaseStats_GTV PurSt on PurD.CustomerID = PerD.CustomerID and PurSt.Recency >= 30 and PurD.Dt_Customer = PurSt.Dt_Customer where PerD.Income > 30000"))

# COMMAND ----------

# DBTITLE 1,Performing inner Join on  PurD with PerD (CustomerID), and then joins based on result of the subquery of PurSt (CustomerID, Month, TotalSpending, and AvgSpending) where Year is 2014.
# Inner Join on  PurD with PerD (CustomerID), and then joins based on result of the subquery of PurSt (CustomerID, Month, TotalSpending, and AvgSpending) where Year is 2014.

display(spark.sql("""
                  select PurD.CustomerID, PurD.Dt_Customer, PurD.Recency, PurSt.Month, sum(PurSt.TotalSpending) as TotalSpending, avg(PurSt.AvgSpending) as AvgSpending,
                  case
                  when PurSt.Month = '1' or PurSt.Month = '2' or PurSt.Month = '3' then 'Quater-1'
                  when PurSt.Month = '4' or PurSt.Month = '5' or PurSt.Month = '6' then 'Quater-2'
                  when PurSt.Month = '7' or PurSt.Month = '8' or PurSt.Month = '9' then 'Quater-3'
                  when PurSt.Month = '10' or PurSt.Month = '11' or PurSt.Month = '12' then 'Quater-4'
                  else "NA"
                  end as Quater
                  from global_temp.PurchaseDetails_GTV as PurD
                  join global_temp.PersonalDetails_GTV as PerD on(PurD.CustomerID = PerD.CustomerID)
                  right outer join 
                  (select CustomerID, Month, TotalSpending, AvgSpending from global_temp.PurchaseStats_GTV where Year = 2014) as PurSt
                  on PurD.CustomerID = PurSt.CustomerID where PerD.Education = 'Graduation' group by 
                  PurD.CustomerID, PurD.Dt_Customer, PurD.Recency, PerD.Education, PurSt.Month
          """))

# COMMAND ----------

# DBTITLE 1,Performing case statements based on Recency, RewardPoints.
refine_df = spark.sql("""
          select CustomerID,
          case
          when Recency <= 30 and MntWines > 100 then "Premium Wine Buyer"
          when Recency <= 30 and MntMeatProducts between 100 and 300 then "Premium Meat Buyer"
          when Recency <= 30 and MntSweetProducts between 1 and 100 then "Frequent Sweets Buyer"
          when Recency <=15 and MntGoldProds between 100 and 500 then "Executive Customers, Cashback applicable"
          else "Occasional Customer - No Discounts"
          end as Buyer_Ctg,
          case
          when RewardPoints_Wine >= 40 then "Wine Enthusiast"
          when RewardPoints_Meat >= 50 then "Meat Lover"
          when RewardPoints_Sweets >= 30 then "Sweet Tooth"
          when RewardPoints_Gold >= 75 then "Gold Buyer"
          else "Regular Customer"
          end as Customer_Rwd_Ctg,
          case
          when RewardPoints_Wine >= 40 then MntWines * 0.90
          when RewardPoints_Meat >= 20 then MntMeatProducts * 0.80
          when MntGoldProds >= 200 or RewardPoints_Gold >= 3 then MntGoldProds * 0.95
          else 0.00
          end as Discounts,
          case
          when Buyer_Ctg = "Occasional Customer - No Discounts" and Discounts > 1200.00 then Discounts - 500
          else Discounts
          end as Fin_Discounts
          from global_temp.PurchaseDetails_GTV
          """)

# COMMAND ----------

# DBTITLE 1,Creating the table from the above dataframe (refine_df).

refine_df.createOrReplaceGlobalTempView("final_table")
refine_df.write.format("delta").saveAsTable("databricksworkspace1.default.refine_table")

# COMMAND ----------

# DBTITLE 1,Performing join operation on PerPurD, PurSt and refine_table.
final_dataframe = spark.sql("select PerPurD.*, PurSt.NumDealsPurchases, PurSt.NumWebPurchases, PurSt.NumCatalogPurchases, PurSt.NumStorePurchases, PurSt.NumWebVisitsMonth, Rt.Buyer_Ctg, Rt.Customer_Rwd_Ctg, Rt.Discounts, Rt.Fin_Discounts, MGTV.TotalDealSales, MGTV.TotalWebSales, MGTV.TotalInStoreSales, MGTV.TotalCatalogSales from databricksworkspace1.default.personal_purchase_details as PerPurD join databricksworkspace1.default.customer_purchasestatistics as PurSt using (CustomerID) join databricksworkspace1.default.refine_table as Rt using (CustomerID) join global_temp.Month_stats_GTV as MGTV using(CustomerID)")

final_dataframe.write.format("delta").saveAsTable("databricksworkspace1.default.Final_Marketing_Table")

display(spark.sql("select * from databricksworkspace1.default.Final_Marketing_Table"))

# COMMAND ----------

# DBTITLE 1,Displaying the contents of the above executed query.
display(spark.sql("select PerPurD.*, PurSt.NumDealsPurchases, PurSt.NumWebPurchases, PurSt.NumCatalogPurchases, PurSt.NumStorePurchases, PurSt.NumWebVisitsMonth, Rt.Buyer_Ctg, Rt.Customer_Rwd_Ctg, Rt.Discounts, Rt.Fin_Discounts, MGTV.TotalDealSales, MGTV.TotalWebSales, MGTV.TotalInStoreSales, MGTV.TotalCatalogSales from databricksworkspace1.default.personal_purchase_details as PerPurD join databricksworkspace1.default.customer_purchasestatistics as PurSt using (CustomerID) join databricksworkspace1.default.refine_table as Rt using (CustomerID) join global_temp.Month_stats_GTV as MGTV using(CustomerID)"))

# COMMAND ----------

# DBTITLE 1,Performing the case statements on Recency and Total_Amt_Spend.
display(spark.sql("""
          select CustomerID, Recency, (MntWines + MntFruits + MntMeatProducts + MntFishProducts + MntSweetProducts + MntGoldProds) as Total_Amt_Spend,
          case
          when Recency <= 30 then "Frequent Buyer"
          when Recency > 30 and Recency < 90 then
          case
          when (MntWines + MntFruits + MntMeatProducts + MntFishProducts + MntSweetProducts + MntGoldProds) > 500 then "Regular Buyer"
          else "Low Spend Customer"
          end
          else
          case
          when sum(MntWines) > 100 then "Wine Purchaser"
          when sum(MntMeatProducts + MntFishProducts) > 100 then "More Recency - Meat Buyer"
          when max(MntGoldProds) >= 200 then "More Recency - Executive Customer"
          else "More Recency - Regular Customer"
          end
          end as Customer_Ctg
          from global_temp.PurchaseDetails_GTV
          group by CustomerID, Recency, MntWines, MntFruits, MntMeatProducts, MntFishProducts, MntSweetProducts, MntGoldProds
          """))

# COMMAND ----------

# DBTITLE 1,Performing case statements based on Recency and RewardPoints.
display(spark.sql("""
          select CustomerID,
          case
          when Recency <= 30 and MntWines > 100 then "Premium Wine Buyer"
          when Recency <= 30 and MntMeatProducts between 100 and 300 then "Premium Meat Buyer"
          when Recency <= 30 and MntSweetProducts between 1 and 100 then "Frequent Sweets Buyer"
          when Recency <=15 and MntGoldProds between 100 and 500 then "Executive Customers, Cashback applicable"
          else "Occasional Customer - No Discounts"
          end as Buyer_Ctg,
          case
          when RewardPoints_Wine >= 40 then "Wine Enthusiast"
          when RewardPoints_Meat >= 50 then "Meat Lover"
          when RewardPoints_Sweets >= 30 then "Sweet Tooth"
          when RewardPoints_Gold >= 75 then "Gold Buyer"
          else "Regular Customer"
          end as Customer_Rwd_Ctg,
          case
          when RewardPoints_Wine >= 40 then MntWines * 0.90
          when RewardPoints_Meat >= 20 then MntMeatProducts * 0.80
          when MntGoldProds >= 200 or RewardPoints_Gold >= 3 then MntGoldProds * 0.95
          else 0.00
          end as Discounts
          from global_temp.PurchaseDetails_GTV
          """));

# COMMAND ----------

display(spark.sql("""
          select CustomerID,
          case
          when Recency <= 30 and MntWines > 100 then "Premium Wine Buyer"
          when Recency <= 30 and MntMeatProducts between 100 and 300 then "Premium Meat Buyer"
          when Recency <= 30 and MntSweetProducts between 1 and 100 then "Frequent Sweets Buyer"
          when Recency <=15 and MntGoldProds between 100 and 500 then "Executive Customers, Cashback applicable"
          else "Occasional Customer - No Discounts"
          end as Buyer_Ctg,
          case
          when RewardPoints_Wine >= 40 then "Wine Enthusiast"
          when RewardPoints_Meat >= 50 then "Meat Lover"
          when RewardPoints_Sweets >= 30 then "Sweet Tooth"
          when RewardPoints_Gold >= 75 then "Gold Buyer"
          else "Regular Customer"
          end as Customer_Rwd_Ctg,
          case
          when RewardPoints_Wine >= 40 then MntWines * 0.90
          when RewardPoints_Meat >= 20 then MntMeatProducts * 0.80
          when MntGoldProds >= 200 or RewardPoints_Gold >= 3 then MntGoldProds * 0.95
          else 0.00
          end as Discounts,
          case
          when Buyer_Ctg = "Occasional Customer - No Discounts" and Discounts > 1200.00 then Discounts - 500
          else Discounts
          end as Fin_Discounts
          from global_temp.PurchaseDetails_GTV
          """));

# COMMAND ----------

# DBTITLE 1,Same as the above query.
# refine_df = spark.sql("""
#           select CustomerID,
#           case
#           when Recency <= 30 and MntWines > 100 then "Premium Wine Buyer"
#           when Recency <= 30 and MntMeatProducts between 100 and 300 then "Premium Meat Buyer"
#           when Recency <= 30 and MntSweetProducts between 1 and 100 then "Frequent Sweets Buyer"
#           when Recency <=15 and MntGoldProds between 100 and 500 then "Executive Customers, Cashback applicable"
#           else "Occasional Customer - No Discounts"
#           end as Buyer_Ctg,
#           case
#           when RewardPoints_Wine >= 40 then "Wine Enthusiast"
#           when RewardPoints_Meat >= 50 then "Meat Lover"
#           when RewardPoints_Sweets >= 30 then "Sweet Tooth"
#           when RewardPoints_Gold >= 75 then "Gold Buyer"
#           else "Regular Customer"
#           end as Customer_Rwd_Ctg,
#           case
#           when RewardPoints_Wine >= 40 then MntWines * 0.90
#           when RewardPoints_Meat >= 20 then MntMeatProducts * 0.80
#           when MntGoldProds >= 200 or RewardPoints_Gold >= 3 then MntGoldProds * 0.95
#           else 0.00
#           end as Discounts,
#           case
#           when Buyer_Ctg = "Occasional Customer - No Discounts" and Discounts > 1200.00 then Discounts - 500
#           else Discounts
#           end as Fin_Discounts
#           from global_temp.PurchaseDetails_GTV
#           """)

# COMMAND ----------


# refine_df.createOrReplaceGlobalTempView("final_table")
# refine_df.write.format("delta").saveAsTable("databricksworkspace1.default.refine_table")

# COMMAND ----------

# DBTITLE 1,Performing case statements based on Dt_Customer, RewardPoints.
display(spark.sql("""
          select CustomerID,
          case
          when(MntWines >= 1300 and RewardPoints_Wine >= 40) or (MntMeatProducts >= 1000 and RewardPoints_Meat >= 20) then "Top Buyer: High Rewards Applicable"
          when (MntGoldProds >= 200 and RewardPoints_Gold >= 2)then "Executive Buyer: Highest Rewards Applicable with Rewards"
          else "Other"
          end as PurchaseRewardCategory,
          case
          when month(Dt_Customer) = month(current_date())
          and CashBack_Earned >= 50 then "Enrollment anniversary"
          else "Regular Month"
          end as Spl_CB_Ctg
          from global_temp.PurchaseDetails_GTV
          """))

# COMMAND ----------

# DBTITLE 1,Performing rank and dense rank operations.
display(spark.sql("""
          Select Dt_Customer,NumStorePurchases,CustomerLTV, rank() over (order by NumStorePurchases desc) as StorePurchasesRank, dense_rank() over (order by NumStorePurchases desc) as StorePurchasesDenseRank from global_temp.PurchaseStats_GTV
"""))

# COMMAND ----------

# DBTITLE 1,Performing union all operation PurD, PerD, PurSt.
# 1st query - CustomerID, Dt_Customer, Recency, MntWines, RewardPoints_Wine, and CashBack_Earned from PurD table where Recency < 60(order by MntWines)(Null as for compatibility).
#2nd query - DOB, Education, Marital_Status, and Income from PerD table where Income > 50000.
#3rd query - CustomerID, Dt_Customer, Recency, NumDealsPurchases, NumCatalogPurchases, and NumStorePurchases from PurSt table where NumStorePurchases > 10


display(spark.sql(""" 
          select CustomerID,Dt_Customer,Recency,MntWines,RewardPoints_Wine,CashBack_Earned,null as DOB,null as Education,null as Marital_Status, null as Income,null as NumDealsPurchases,null as NumCatalogPurchases,null as NumStorePurchases from (select CustomerID,Dt_Customer,Recency,MntWines,RewardPoints_Wine,CashBack_Earned from global_temp.PurchaseDetails_GTV where Recency < 60 order by MntWines)

          union all

          select  null as CustomerID,null as Dt_Customer,null as Recency,null as MntWines,null as RewardPoints_Wine,null as CashBack_Earned,DOB,Education,Marital_Status,Income,null as NumDealsPurchases,null as NumCatalogPurchases,null as NumStorePurchases FROM (select DOB, Education,Marital_Status,Income from global_temp.PersonalDetails_GTV where Income > 50000 order by DOB)

          union all

          select CustomerID,Dt_Customer,Recency, null as MntWines,null as RewardPoints_Wine,null as CashBack_Earned,null as DOB,null as Education,null as Marital_Status,null as Income,NumDealsPurchases,NumCatalogPurchases,NumStorePurchases from (select CustomerID,Dt_Customer,Recency,NumDealsPurchases,NumCatalogPurchases,NumStorePurchases from global_temp.PurchaseStats_GTV where NumStorePurchases > 10 order by CustomerID) 
          """))

