// Databricks notebook source
2+2

// COMMAND ----------

var var1 : String = "Prince"

// COMMAND ----------

var var1 ="Prince Singh"

// COMMAND ----------

val var2 :String = "Prince"

// COMMAND ----------

val var2 = "Prince Singh"

// COMMAND ----------

val var3 ="Prince"

// COMMAND ----------

var1="Ankit"

// COMMAND ----------

if (var2=="Ankit"){
  println("Matched")
} else {
  println("Not Matched")
}

// COMMAND ----------

for( a <- 1 to 10){
 println( "Value of a: " + a );
 }

// COMMAND ----------

def mul2(m: Int):Unit = println("value is=" + m * 10)

// COMMAND ----------

mul2(4)

// COMMAND ----------

val data = Array(1, 2, 3, 4, 5,6,7,8,9,10)
val distData = sc.parallelize(data)
distData.collect

// COMMAND ----------

val d1 = sc.textFile("/FileStore/tables/test_data.txt")
d1.collect


// COMMAND ----------

/////Working with Map Types in Spark Scala

import org.apache.spark.sql.functions;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.functions.map_keys;
val d2 = sc.parallelize(
  Seq(
    ("Prince", Map("TV"->2,"FAN"->3)),
    ("Raj", Map("AC"->1)),
    ("Prince", Map("TV"->2))
  ))
//d2.collect
val d3 = spark.createDataFrame(d2)toDF("Name", "vals")
//d3.show
val exploded = d3.select($"Name",$"vals", explode($"vals")).toDF("Name","vals", "Item", "Qty")
//exploded.printSchema
//exploded.show
//val d5 = exploded.select("Name","Item","Qty")
//d5.show
//d5.dropDuplicates.show
//d5.select("Name","Item").show
val d4 = exploded.dropDuplicates("Name","Item","Qty")
d4.show
//val d5= d4.select("Name","vals")
//d5.show
//d3.select("name","vals").show
//d3.select(map_keys($"vals")).show
//val withHashedColumn = d3.withColumn("hashed", hash($"vals"))
//val r1 = d2.distinct
//r1.collect
//val d4 = spark.createDataFrame(r1).toDF("Name", "vals")
//d4.show
//val d4 = spark.createDataFrame(r1)toDF("Name", "vals")
//d4.show
//d3.select("vals".value).show
//d3.distinct.show
//val d4 = d3.withColumn("hashString", functions.hash(col("vals"))
//d4.show

// COMMAND ----------

import sqlContext.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}  

val schema_string = "name,id,dept"
val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )
val empty_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)
empty_df.show

// COMMAND ----------

val df1 = Seq("AAA","BBB","CCC").toDF("Col1")
//df1.show
//val l1 = df1.select("col1")
var l1 = df1.select("col1").collect().map(_(0)).toList
 //df1.select("col1").collect()
val df2 = Seq("ZZZ","ZZX","YYY").toDF("Col1")
l1 = (df2.select("col1").collect().map(_(0)).toList)


// COMMAND ----------

//import org.apache.spark.sql.functions._
//df1.withColumn("rowId", monotonically_increasing_id()).show
//df1.selectPlus(md5(concat(keyCols: _*)) as "uid")

// COMMAND ----------

val df11 =  Seq("AAA","BBB","CCC","").toDF("Col1")
//df11.show
//val df12 = df11.na.fill("a",Seq("Col1"))
//df12.show
val new_df = df11.withColumn("Col11",when(col("Col1")==="",lit("BLA")).otherwise(col(("Col1"))))
new_df.show


// COMMAND ----------

val encodedInt = List.fill(75)("1").mkString("")
BigInt(encodedInt)

// COMMAND ----------

val df11 =  Seq(1111111111111111111111111111111111111111111111111111111,1111111111111111111211111111111111111111111111111111).toDF("Col1")

// COMMAND ----------


val d1 = sc.textFile("/FileStore/tables/testFile.txt").map(line=>line.split(","))
d1.collect
import sqlContext.implicits._
val dfWithoutSchema = d1.toDF()
dfWithoutSchema.show(false)

// COMMAND ----------

import org.apache.spark.sql.types.{LongType,DecimalType}
import org.apache.spark.sql.functions.{col, udf}

val df=spark.read.format("csv").option("header","false").load("/FileStore/tables/testFile2-1.csv")
            .withColumnRenamed("_c0","name")
            .withColumnRenamed("_c1","sal")
val df1 = df.withColumn("sal", col("sal").cast(new DecimalType(38,0)))
df.show(false)
df1.show(false)

df1.write.csv("/FileStore/tables/datacsv")



// COMMAND ----------

val chkDf = spark.read.format("csv").option("header","true").load("/FileStore/tables/datacsv/part-00000-tid-6882148570634068431-df89b478-a889-4311-ba91-2d9c030c7389-39-1-c000.csv")
chkDf.show(false)