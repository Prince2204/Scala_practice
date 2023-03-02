// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

println("datafrom stage table")
val regDF = Seq(("region1", 1),("region1", 2),("region1", 3),("region2", 1),
                ("region2", 2),("region2", 3),("region3", 3),("region4", 2)).toDF("region", "version")
regDF.show()

println("datafrom version table")
val regDF1 = Seq(("region1", 2),("region2", 5),("region3", 3),("region4", 1)).toDF("region", "version")
val regDF2 = regDF1.withColumn("flag1",lit("Y"))
regDF2.show()

val joinDF = regDF.join(regDF2,regDF("region")===regDF2("region") && regDF("version")===regDF2("version"),"left_outer")
             .select(regDF.col("*"),regDF2.col("flag1"))
             .withColumn("flag1",coalesce(regDF2.col("flag1"),lit("N")))
println("datafrom joinDF table")
joinDF.show()

println("data for matching keys ")
val flagData = joinDF.filter(col("flag1") === 'Y').drop("flag1")
println("data for  matching regions ")
flagData.show()
val nonFlagData = regDF.join(flagData,regDF("region")===flagData("region") ,"leftanti")
             .select(regDF.col("*"))
//val regList = flagData.select("region").collect
//println("data for matching regions ")
//val regList = flagData.select("region").collect().map(_(0)).toSet.toSeq
//nonFlagData.show()
//val nonFLagData = regDF.filter(col("region") isNotin (regList: _*))
println("data for non matching regions ")
nonFlagData.show()
val verWindow = Window.partitionBy("region").orderBy(col("version").desc)
println("data for non matching regions final")
val nonFlagVErsion = nonFlagData.withColumn("ranking", dense_rank().over(verWindow))
          .filter(col("ranking") === 1)
          .drop("ranking")
nonFlagVErsion.show()
val finalData = nonFlagVErsion.union(flagData)
println("---- finaldata---")
finalData.show()

// COMMAND ----------

import org.apache.spark.sql.functions._

println("datafrom stage table")
val regDF = Seq(("region1", 1),("region1", 2),("region1", 3),("region2", 1),
                ("region2", 2),("region2", 3),("region3", 3),("region4", 2)).toDF("region", "version")
regDF.show()

println("datafrom version table")
val regDF1 = Seq(("region1", 2),("region2", 5),("region3", 3),("region4", 1)).toDF("region", "version")
val regDF2 = regDF1.withColumn("flag1",lit("Y"))
regDF2.show()

val joinDF = regDF.join(regDF1,regDF("region")===regDF1("region") && regDF("version")===regDF1("version"),"leftanti")
             .select(regDF.col("*"))

joinDF.show()

// COMMAND ----------

import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val df1 = Seq(("hamsa","0001-01-01"),("prince","9999-12-31")).toDF("name","date") 
val df2 = df1.withColumn("date", col("date").cast(DateType)) 
df2.createOrReplaceTempView("TEstTable")
val df3 = spark.sql("""select * from TEstTable""")
df3.show()

// COMMAND ----------

