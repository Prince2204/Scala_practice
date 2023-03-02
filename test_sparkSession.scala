// Databricks notebook source
spark

// COMMAND ----------

spark.sparkContext

// COMMAND ----------

//spark.sqlContext
spark.conf.get("spark.sql.warehouse.dir")

// COMMAND ----------

import org.apache.spark.sql.functions._
val numDS = spark.range(5, 100, 5)
//numDS.show
//numDS.orderBy(desc("id")).show
display(numDS.describe())

// COMMAND ----------

//val df = spark.read.json("/FileStore/tables/employee.json",multiLine=True)
val df = spark.read.option("multiLine", true).json("/FileStore/tables/employee.json")

df.show()

// COMMAND ----------

//FileStore/tables/test_4_03_19.csv
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkContext
import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{DateType, StringType}
val df = spark.read.option("header", true).csv("/FileStore/tables/test_4_03_19.csv")
df.show
val sc = StructType(StructField("s1",StringType,true)::StructField("d1",StringType,true)::StructField("t1",StringType,true)::Nil)
//val df1 = df.select("bu","comm",struct("s1","d1","t1").alias("m1"))
val df1 = df.select("bu","comm")
df1.show


// COMMAND ----------

