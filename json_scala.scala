// Databricks notebook source
//from pyspark.sql.functions import explode,col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
val df = spark.read.option("multiLine", true).json("/FileStore/tables/test.json")
//df.printSchema
//df.show
val df1 = df.select("ADDRESS","TIES","FILTERS")
//df1.show
val df2 = df1.withColumn("ADDRESS",explode(col("ADDRESS")))
              .withColumn("TIES",explode(col("TIES")))
//              .withColumn("FILTERS",explode(col("FILTERS")))
df2.show
val df3 = df2.withColumn("BASE_SYSTEM_CODE",col("TIES.ATTRIBUTES.BASE_SYSTEM_CODE"))
            .withColumn("TIE_NUM",col("TIES.TIE_NUM")).drop("TIES")
df3.show
//df2.select("TIES.ATTRIBUTES.BASE_SYSTEM_CODE" as BSC).show
//df2.select("ADDRESS","TIES.ATTRIBUTES.BASE_SYSTEM_CODE as BSC" ,"TIES.TIE_NUM as TNUM", "FILTERS.WO[0]").show

// COMMAND ----------

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
val df = spark.read.option("multiLine", true).json("/FileStore/tables/test.json")
df.printSchema
df.show
val flatdf = df.select("DEMAND_REGION",
      "_id",
      "FINANCE_ATTRIBS.BUID",
      "TIES",
      "ADDRESS",
      "ROUTING_INFO.EST_DELIVERY_DATE",
      "ROUTING_INFO.MUST_SHIP_BY_DATE",
      "ORDERAMOUNT",
      "SO_IDENTIFIER.ORDER_TYPE",
      "SYSTEM_QTY",
	  "FILTERS.WO")
      .withColumn("source", lit("FSL"))
      .withColumn("sales_order_num", $"_id".cast("Long")).drop("_id")
      .withColumn("buid", $"BUID".cast("int"))
      .withColumn("ORDERAMOUNT", $"ORDERAMOUNT".cast("Float"))
      .withColumn("status", lit(null))
      .withColumn("SYSTEM_QTY", $"SYSTEM_QTY".cast("int")) 
//flatdf.printSchema
//flatdf.show
val df2 = flatdf.select($"DEMAND_REGION",$"sales_order_num",$"buid",$"EST_DELIVERY_DATE",$"MUST_SHIP_BY_DATE",$"ORDERAMOUNT",$"ORDER_TYPE",$"SYSTEM_QTY"                      ,$"WO",$"source",$"status",explode($"TIES").alias("TIES")).
       select($"DEMAND_REGION",$"sales_order_num",$"buid",$"EST_DELIVERY_DATE",$"MUST_SHIP_BY_DATE",$"ORDERAMOUNT",$"ORDER_TYPE",$"SYSTEM_QTY"                      ,$"WO",$"source",$"status",$"TIES.TIE_NUM",$"TIES.ATTRIBUTES.BASE_SYSTEM_CODE")
df2.printSchema
df2.show


// COMMAND ----------

val df3 = flatdf.select($"sales_order_num",$"ADDRESS",explode($"TIES").alias("TIES"))
          .select($"sales_order_num",$"TIES.TIE_NUM" as "TIE_NUM",$"TIES.ATTRIBUTES.BASE_SYSTEM_CODE" as "BASE_SYSTEM_CODE",explode($"ADDRESS").alias("ADDRESS")).select($"sales_order_num",$"TIE_NUM",$"BASE_SYSTEM_CODE",$"ADDRESS.COMPANY_NAME" as "CNAME")

val df4 = df3.filter($"CNAME".isNotNull).createOrReplaceTempView("tempTable")
spark.sql("""select * from tempTable""").show       
//df3.show
//df.printSchema

//.select($"sales_order_num",$"ADDRESS.COMPANY_NAME",$"TIES.TIE_NUM",$"ATTRIBUTES.BASE_SYSTEM_CODE")  

// COMMAND ----------

