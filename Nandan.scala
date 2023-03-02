// Databricks notebook source
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
import scala.collection.mutable.WrappedArray
val df1 = spark.read.format("csv").option("header","true").load("/FileStore/tables/orderType.csv")
//df1.printSchema
val df2 = df1.groupBy("region").agg(collect_set("so_num") as "list_SO_number").rdd.map(r=>(r(0).toString,r(1).toString)).collect.toList

for(r1<- df2){
  println(r1.select(concat_ws(",", col("region"),col("list_SO_number"))))
println(r1._1)
println(r1._2.toString())
}

//df2.show
//val rdd1 = df2.rdd.collect.toList
//val rdd1 = df1.rdd.map(r=>(r(0).toString,r(1).toString))
//val rdd2 = rdd1.groupByKey()
//rdd2.collect
//rdd2.map(r=> {
//  val region = r._1
//  println(region)
//  val so_list = r._2
//  so_list.foreach(println)
//})


// COMMAND ----------

import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
import scala.collection.mutable.WrappedArray
import spark.implicits._

val pi = spark.read.format("csv").option("header","true").load("/FileStore/tables/pi.csv")
val dem = spark.read.format("csv").option("header","true").load("/FileStore/tables/dem.csv")
pi.show
dem.show
val colNames = pi.schema.fieldNames.filter(x=> x !="col1" & x!="col2")
val demCols = colNames.map(name => dem.col(name) as name.concat("_pi"))
//val selectCols = colNames.map(name => pi.col(name)/dem.col(name) as name)
val jdf = pi.join(dem,pi("col1")===dem("col1") && pi("col2")===dem("col2")).select(demCols:_*)
jdf.show
//var x = 4 
//dem.select(while (x<4) {
//  dem.columns(x)
//  x = x-1
//}).show
//val cols = dem.columns
//println(dem.columns(2))
//pi.join(dem,pi("col1")===dem("col1") && pi("col2")===dem("col2")).select(dem.col(_*)).show
//val selCol = colNames.map(name => pi.col(name)/dem.col(name) as name)
//pi.join(dem,pi("col1")===dem("col1") && pi("col2")===dem("col2")).select(selectCols:_*).show
//pi.join(dem,pi("col1")===dem("col1") && pi("col2")===dem("col2")).select(pi("col1")).show
   //   .select(dem.columns(0),dem.columns(1),pi.columns(2)).show
        //      pi.columns(2)/(dem.columns(2)+dem.columns(3))*20 as pi.columns(2)
          //                  ).show
//dem.select("_1").show
//pi.join(dem,pi("col1")===dem("col1")).show

// COMMAND ----------

import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
import scala.collection.mutable.WrappedArray
import spark.implicits._

val pi = spark.read.format("csv").option("header","true").load("/FileStore/tables/pi.csv")
pi.show
//pi.write.format("TEST").save("/FileStore/tables/users_with_options.csv")
println("file saved")

val d2 = spark.read.format("csv").option("header","true").load("/FileStore/tables/users_with_options.csv/part-00000-tid-6242940837582026900-a95b7ed0-ac4e-47c5-8a16-0dee010cafe3-4-1-c000.csv")
d2.show


// COMMAND ----------

