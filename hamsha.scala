// Databricks notebook source
val finalSummedDemanDf = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("header", "true")
      .option("inferschema", "true")
      .load("/FileStore/tables/demand.txt")
val suffix = "De"
val renamedColumns = finalSummedDemanDf.columns.map(c=> finalSummedDemanDf(c).as(c.concat(suffix)))
val finalSummedDemanDf1 = finalSummedDemanDf.select(renamedColumns: _*)
finalSummedDemanDf1.show(2)

val DemandSupplyCumulativeSum = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("header", "true")
      .option("inferschema", "true")
      .load("/FileStore/tables/PI.txt")
DemandSupplyCumulativeSum.show(2)

// COMMAND ----------


      var a = true;
      var b = false;

      println("a && b = " + (a&b) );
      
      println("a || b = " + (a||b) );
      
      println("!(a && b) = " + !(a && b) );
