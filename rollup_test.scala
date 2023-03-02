// Databricks notebook source
//import org.apache.spark.sql._
import org.apache.spark.sql.functions._
val sales = Seq(
  ("Warsaw","AAA", 2016, 100),
  ("Warsaw","BBB", 2017, 200),
  ("Boston","AAA", 2015, 50),
  ("Boston","AAA", 2016, 150),
  ("Toronto","BBB", 2017, 50)
).toDF("city","QQQ", "year", "amount")

val withRollup = sales
  .rollup("city","QQQ", "year")
  .agg(sum("amount") as "amount",grouping_id() as "gid")
  .select("city","QQQ","year", "amount","gid")

withRollup.show

// COMMAND ----------

import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
val df1 = spark.read.format("csv").option("header","true").load("/FileStore/tables/filt1.csv")
df1.show
//df1.groupBy("qqq").pivot("www").agg(collect_list("www")).na.fill("-").show
//val df2 = df1.withColumn("www",when($"www"==="0","AAA").otherwise("na"))
//df2.show

// COMMAND ----------

import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
val df1 = spark.read.format("csv").option("header","true").load("/FileStore/tables/filt1.csv")
df1.show
val df2 = df1.filter(col("www")===2)
df2.show
if (df2.take(1).nonEmpty){
  val df3 = df1.union(df2)
df3.show
}

//val df2 = df1.filter

// COMMAND ----------

