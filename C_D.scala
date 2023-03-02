// Databricks notebook source
///FileStore/tables/C_D.csv
//import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
val df1 = spark.read.format("csv").option("header","true").load("/FileStore/tables/C_D.csv")
//df1.show
val df_1 = df1.withColumn("Measure",df1("Measure").cast("int"))
 //               .withColumn("start_date",df1("start_date").cast("Date"))
df_1.printSchema
//df_1.show
val df2 = df_1.groupBy("site","ccn").pivot("start_date").sum("Measure")
//df2.show
//df2.printSchema
//val cols1 = df2.columns
//val mapColumn = map(df2.columns.tail.flatMap(name => Seq(lit(name.toLong), $"$name")): _*)
val mapKeys = df2.columns.filterNot(_ == "site").filterNot(_ == "ccn")
val pairs = mapKeys.map(k => Seq(lit(k), col(k))).flatten
val mapped = df2.select($"site",$"ccn", functions.map(pairs:_*) as "dateval")
mapped.show(false) 
//df2.select(cols.head, cols.tail: _*)
//var colnms_n_vals = df2.columns.flatMap { c => Array(lit(c), col(c)) }
//df2.withColumn("newcol", struct(df2.columns.head, df2.columns.tail: _*)).show

// COMMAND ----------

///FileStore/tables/C_D.csv
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
val df1 = spark.read.format("csv").option("header","true").load("/FileStore/tables/C_D.csv")


// COMMAND ----------

// /FileStore/tables/df_iterate.csv
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
import scala.collection.JavaConversions._
val df1 = spark.read.format("csv").option("header","true").load("/FileStore/tables/df_iterate.csv")
df1.show
val df2 = df1.groupBy("cust").agg(collect_set("order").alias("order_list"))
df2.show
df2.printSchema
var df3 =df2.collectAsList
//var df3 =df2.rdd
//df3.collect
df3.foreach {row => 
  var itemList = row.getSeq[String](1).toArray
  println("list of items is" +itemList.mkString("'","','","'"))
  
}

// COMMAND ----------

//File uploaded to /FileStore/tables/file1.csv
//File uploaded to /FileStore/tables/file2.csv

import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
val df1 = spark.read.format("csv").option("header","true").load("/FileStore/tables/testdata").withColumn("file_name",input_file_name())
val df2 = df1.withColumn("file_name",split(col("file_name"),"/").getItem(4))
df2.show(false)

// COMMAND ----------

