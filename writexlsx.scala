// Databricks notebook source
println("datafrom stage table")
val regDF = Seq(("region1", 1),("region1", 2),("region1", 3),("region2", 1),
                ("region2", 2),("region2", 3),("region3", 3),("region4", 2)).toDF("region", "version")
regDF.show()

regDF.repartition(1).write.format("com.databricks.spark.csv")
   .option("header", "true")
   .save("mydata.csv")

// COMMAND ----------

println("datafrom stage table")
val regDF = Seq(("region1", 1),("region1", 2),("region1", 3),("region2", 1),
                ("region2", 2),("region2", 3),("region3", 3),("region4", 2)).toDF("region", "version")
regDF.show()

regDF.write
  .format("com.crealytics.spark.excel")
  .option("dataAddress", "'My Sheet'!A1")
  .option("useHeader", "true")
  .option("header", "true")
  .mode("overwrite") // Optional, default: overwrite.
  .save("/mydata1.xlsx")