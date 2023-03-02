# Databricks notebook source
df = spark.read.json('/FileStore/tables/test.json',multiLine=True)

# COMMAND ----------

from pyspark.sql.functions import explode,col
df1 = df.select(explode("ADDRESS").alias("Address"),"CUSTOMER_NAME","DEMAND_REGION",col("FILTERS.WO").alias("WO"),col("FINANCE_ATTRIBS.BUID").alias("BUID"),col("FINANCE_ATTRIBS.CURRENCY").alias("CURRENCY"),"ORDERAMOUNT","REGION",col("ROUTING_INFO.EST_DELIVERY_DATE").alias("EST_DELIVERY_DATE"), col("ROUTING_INFO.MUST_SHIP_BY_DATE").alias("MUST_SHIP_BY_DATE"), col("SO_IDENTIFIER.ORDER_TYPE").alias("ORDER_TYPE"),"SYSTEM_QTY","TIES")

# COMMAND ----------

