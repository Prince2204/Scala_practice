// Databricks notebook source
import scala.util.parsing.json._

val parsed = JSON.parseFull("""{"Name":"abc", "age":10}""")

// COMMAND ----------

import org.apache.spark.sql.functions.to_json

val someDF = Seq(
  (8, "bat","Prince",Map("abc"->"001","xyz"->"002")),
  (64, "mouse","Shivam",Map("abc"->"002")),
  (27, "horse","Praveen",Map("abc"->"003"))
).toDF("number", "word","Name","RollNo")

someDF.show(false)
someDF.printSchema

//val jsonData = someDF.select($"number", $"word", $"Name",struct($"RollNo").alias("RollNo"))

//jsonData.show(false)
//jsonData.printSchema

//val finJson = jsonData.select($"number", $"word",$"Name",to_json($"RollNo").alias("RollNo"))
val finJson = someDF.withColumn("RollNo", to_json($"RollNo"))

//jsonData.select($"number", $"word",$"Name",to_json($"RollNo").alias("RollNo"))

finJson.show(false)
finJson.printSchema

//finJson.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/df/jsonTesting4.csv")

// COMMAND ----------

val sparkDF = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("dbfs:/FileStore/df/jsonTesting4.csv")

sparkDF.show(false)
sparkDF.printSchema

//val df1 = sparkDF.withColumnRenamed("structstojson(test1)","jsonData")
//df1.show(false)
//df1.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.{lit, schema_of_json}

val schema = new StructType()
  .add($"Name".string)
  .add($"RollNo".string)

schema.printTreeString

val mySchema = StructType(Seq(
   StructField("RollNo", MapType(StringType, StringType, true), true)))

//mySchema.printTreeString
//val schema = schema_of_json(lit(df1.select($"jsonData").as[String].first))
//println(schema)
val df2 = sparkDF.withColumn("jsonData1", from_json($"RollNo", mySchema)) 
df2.show(false)

// COMMAND ----------

import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val someDF = Seq(
  (8, "bat","Prince","001"),
  (64, "mouse","Shivam","002"),
  (27, "horse","Praveen","003")
).toDF("number", "word","Name","RollNo")

someDF.show

//val dfStruct = df.select($"number", struct($"C1", $"C2", $"C3").alias("C"))

val jsonData = someDF.select($"number", $"word", struct($"Name", $"RollNo").alias("test1"))

jsonData.show
jsonData.printSchema

val finJson = jsonData.select($"number", $"word",to_json($"test1"))

finJson.show(false)
finJson.printSchema



//finJson.createOrReplaceTempView("TempTable")

// COMMAND ----------

finJson.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/df/jsonTesting.csv")

// COMMAND ----------

val sparkDF = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("dbfs:/FileStore/df/jsonTesting.csv")

val df1 = sparkDF.withColumnRenamed("structstojson(test1)","jsonData")
df1.show(false)
df1.printSchema



//"dbfs:/FileStore/df/jsonTesting.csv"

// COMMAND ----------

import org.apache.spark.sql.functions.{lit, schema_of_json}

val schema = new StructType()
  .add($"Name".string)
  .add($"RollNo".string)

schema.printTreeString

val mySchema = StructType(Seq(
   StructField("mapData", MapType(StringType, StringType, true), true)))

//mySchema.printTreeString
//val schema = schema_of_json(lit(df1.select($"jsonData").as[String].first))
//println(schema)
val df2 = df1.withColumn("jsonData1", from_json($"jsonData", schema)) 
df2.show(false)

