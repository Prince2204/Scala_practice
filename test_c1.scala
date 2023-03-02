// Databricks notebook source
import scala.math.BigDecimal
println("hi")
println(BigDecimal(1.23456789).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

val df = Seq(
  ("a", 2, 3, 5, 3, 4, 2, 6, 7, 3),
  ("b", 5, 7, 3, 6, 8, 8, 9, 4, 2),
  ("c", 1, 2, 3, 4, 5, 6, 7, 8, 9),
  ("a", 1, 1, 2, 4, 5, 7, 3, 5, 2),
  ("b", 2, 2, 3, 5, 6, 3, 2, 4, 8),
  ("c", 2, 3, 4, 5, 6, 7, 8, 9, 5)
).toDF("id", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9")

val col_list = df.columns.filter(x => x !="id")
val windowSpec = Window.orderBy("id")
val ops2 = col_list.map(k=>lag(df(s"$k"),1,0).over(windowSpec))
ops2.foreach(println)
//val ops1 = col_list.map(k=>(df(s"$k") - lag(df(s"$k"),1,0).over(windowSpec)))
//ops1.foreach(println)
val df1 = df.groupBy("id")
    .agg(ops2.head,ops2.tail:_*)


val df2 = Seq(
  ("a", 1, 1, 2, 4, 5, 7, 3, 5, 2),
  ("b", 2, 2, 3, 5, 6, 3, 2, 4, 8),
  ("c", 2, 3, 4, 5, 6, 7, 8, 9, 5)
).toDF("id", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9")


//val ops = col_list.map(k=>df(s"$k")-df2(s"$k") as s"$k")
//ops.foreach(println)
//val df4 = df.join(df2,"id").select(ops.foreach(x=>x))
//df4.show
//ops1.foreach(println)
//println(ops.head)
//val df1 = df.groupBy("id").agg(ops1.head,ops1.tail:_*)
//val df4 = df.join(df2,"id")
  //        .withColumn()
//.agg(ops.head,ops.tail:_*)
//.select(df("id"),ops.head,ops.tail)
//df4.show
//val df3 = df.join(df2,"id").
//            select(df("id"),df("p1")-df2("p1") as "p1")
// df3.show
//dfg.show
//val col_list = df.columns.filter(x => x !="id")
//val ops = col_list.map(k => sum(df(s"$k")).alias(s"$k"))
//val windowSpec = Window.partitionBy("id").orderBy("id")
//val ops1 = col_list.map(k => lag(df(s"$k"),1).over(windowSpec).alias(s"$k"))
//ops.foreach(println)
//ops1.foreach(println)
//val df1 = df.groupBy("id")
//    .agg(ops1.head,ops1.tail:_*)
//df1.show
//col_list.foreach(println)
//val pcols = List.range(1, 10)
//val ops = pcols.map(k => sum(df(s"p$k")).alias(s"p$k"))
//val windowSpec = Window.partitionBy("id").orderBy("id")
//df.withColumn(col_list.map(k=>k.toString), lag("Amount", 1).over(windowSpec))
//   .show(false)

// COMMAND ----------

var data = " Data/Data"
var data1 = "Prince"
var data2 = "Data/Data"
if(data==data1) println(" data = data1")
else if (data.trim().equals(data2.trim())) println(" data = data2")
else println("not matched")

// COMMAND ----------

