// Databricks notebook source
///FileStore/tables/employee.txt
// defining the case class
case class Employee(id: Int, name: String, age: Int, city: String)
// Reading the text file
val empl=sc.textFile("/FileStore/tables/employee.txt")
// Split the files into lines and then map each line to the case class Employee and convert it to dataframe
val empl1=empl.map(_.split(",")).map(e⇒ Employee(e(0).trim.toInt,e(1), e(2).trim.toInt,e(3))).toDF()
empl1.show
empl1.select(size(empl1.data)).collect()
//empl1.groupBy('age % 2)
// Registering it to the temp table
//empl1.createOrReplaceTempView("employee")
//selecting the records from the temp table
//val allrecords = sqlContext.sql("SELeCT * FROM employee")
//allrecords.show

// COMMAND ----------

//val rdd=spark.sparkContext.parallelize(Seq((1,2,3),(4,5,6,7),(7,2,6,9,10)))
var mark = sc.parallelize(List(1,2,3,4,5,6))
mark.collect()
val x = List(List("xxx","iii","ppp"), List("vvvv","ooo","ggg","ppp"), List("aaa","bbb","ccc"),List("ppp","qqq","rrr","sss","www"),List("ppp","ooo"))
val rdd = sc.parallelize(x)
//rdd.collect()

val rdd_length = rdd.map(x =>(x,x.length))
//rdd_length.collect()
val rdd1 = rdd_length.filter(x=>x._2 !=3)
rdd1.collect()

//rdd.foreach(println)
//val rdd = sc.parallelize(x)
//val rdd_length = rdd.map(lambda x: len(x))
//rdd_length.collect()

// COMMAND ----------

//FileStore/tables/employee.json
//val dfs = sqlContext.read.json("/FileStore/tables/employee.json")
//dfs.show
//val dfs1 = dfs.select("name","id","age").where(dfs("age") isNotNull)
//dfs1.coalesce(1).write.format("csv").option("header", "true").save("/FileStore/tables/sample_emp_out1.csv")
//val dfs2 = sqlContext.read.format("csv").option("header", "true").load("/FileStore/tables/sample_emp_out1.csv")
//dfs2.show
//dfs2.select("name").groupBy(dfs2("age")).count().show

// COMMAND ----------

///FileStore/tables/test.json
//val fsl = spark.read.json("/FileStore/tables/test.json")
val d1 = sc.textFile("/FileStore/tables/test.json")
d1.collect
//fsl.printSchema
//fsl.show

// COMMAND ----------

//FileStore/tables/test_data.csv
import org.apache.spark.sql.{Dataset} 
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
val rdd = spark.read.option("header","true").csv("/FileStore/tables/test_data.csv")
val rdd1 = rdd.groupBy("value").agg(collect_set("desc") as "descset")
rdd1.show
rdd1.printSchema
//val lobVal = (rdd.select("desc").collect().map(_(0)).toSet).toDF
//val rdd1 = rdd.select(rdd.select("desc").collect().map(_(0)).toSet).alias("set_val")
//rdd.show
//val lobVal:(Dataset[Row] => Set[String]) = rdd.select("desc").collect().map(_(0)).toList
//val sqlfunc = udf(lobVal)
//val rdd1 = rdd.withColumn("desc1",sqlfunc(col("desc")))
//rdd1.show
//val lobVal = rdd.select("desc").collect().map(_(0)).toSet
//rdd1.show
//rdd.select("desc").collect().map(_(0)).toSet

// COMMAND ----------

import org.apache.spark.sql.functions._
val num = spark.range(10)
num.show
//num.groupBy('id % 2 as "group").agg(sum('id) as "sum").show
val ids = num.agg(sum('id) as "sums")
ids.show
//num.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._
val columns = Seq("sourceId","score_1","score_2","score_3","score_4")
val data = Seq((1, 11,12,13,14), (2, 21,22,23,24), (3,31,32,33,34),(5,41,42,43,44))
val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF(columns:_*)
dfFromRDD1.printSchema()
dfFromRDD1.show()

val ids = Seq(1, 3, 4, 2)

val scoreCol = ids.foldLeft(lit(null)) { case (acc, id) =>
  when(col("sourceId")===id, col(s"score_$id")).otherwise(acc)
}

val df2 = dfFromRDD1.withColumn("score", scoreCol)
df2.show()



// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._
val columns = Seq("sourceId","score_1","score_2","score_3","score_4")
val data = Seq((1, 11,12,13,14), (2, 21,22,23,24), (3,31,32,33,34),(5,41,42,43,44))
val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF(columns:_*)
dfFromRDD1.printSchema()
dfFromRDD1.show()

//val ids = Seq(1, 3, 4, 2)

//val scoreCol = ids.foldLeft(lit(null)) { case (acc, id) =>
//  when(col("sourceId")===id, col(s"score_$id")).otherwise(acc)
//}

//val df2 = dfFromRDD1.withColumn("score", scoreCol)
//df2.show()

println("printing " + dfFromRDD1.columns.filter(_.startsWith("score_")))
val l1 = dfFromRDD1.columns.filter(_.startsWith("score_"))

//val ids_map = Map("score_1" -> 1, "score_1" -> 2, "score_3" -> 3 , "score_4"->4)

//val scoreMap1 = map(ids_map: _*)

val scoreMap = map(l1
    .flatMap(c => Seq(lit(c.split("_")(1)), col(c))): _*
)

scoreMap



val df2 = dfFromRDD1.withColumn("score", scoreMap(col("sourceId")))

//val df2 = dfFromRDD1.withColumn("score", col("sourceId").map)

df2.show()



// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._
val columns = Seq("fileCrtDt","wk_1_qty","wk_2_qty","wk_3_qty","wk_4_qty","wk_5_qty","week_seq")
//val columns = Seq("fileCrtDt","wk_1","wk_2","wk_3","wk_4","wk_5","week_seq")
val data = Seq(("2022-03-25", 11,12,13,14,15,2), ("2022-03-18", 21,22,23,24,25,3), ("2022-03-11",31,32,33,34,35,4),("2022-03-04",41,42,43,44,45,5))
val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF(columns:_*)
dfFromRDD1.printSchema()
dfFromRDD1.show()

val ids = Seq(1,2,3,4,5)

val scoreCol = ids.foldLeft(lit(null)) { case (acc, id) =>
  when(col("week_seq")===id, col(f"wk_$id%s_qty")).otherwise(acc)
  //when(col("week_seq")===id, col(fs"wk_$id")).otherwise(acc)
}

val df2 = dfFromRDD1.withColumn("week_value", scoreCol)
df2.show()





//d2.show()
                               





// COMMAND ----------

import java.util.Calendar

val cal = Calendar.getInstance()
cal.setTime('2022-03-30')
cal.add(Calendar.WEEK_OF_YEAR, -1)
cal.set(Calendar.DAY_OF_WEEK, day)
println(cal.getTime())


// COMMAND ----------



// COMMAND ----------

val ids_map = Map("2022-03-25" -> "wk_2_qty", "2022-03-18" -> "wk_3_qty", "2022-03-11" -> "wk_4_qty" , "2022-03-04"->"wk_5_qty")
val result = show(ids_map.get("2022-03-25"))

def show(x: Option[String]): String = x match {
      case Some(s) => s
      case None => "default_week"
   }
print(result)

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._

// get the last 26 fridays

val columns = Seq("fileCrtDt","wk_1_qty","wk_2_qty","wk_3_qty","wk_4_qty","wk_5_qty","week_seq")
//val columns = Seq("fileCrtDt","wk_1","wk_2","wk_3","wk_4","wk_5","week_seq")
val data = Seq(("2022-03-25", 11,12,13,14,15,2), ("2022-03-18", 21,22,23,24,25,3), ("2022-03-11",31,32,33,34,35,4),("2022-03-04",41,42,43,44,45,5))
val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF(columns:_*)
dfFromRDD1.printSchema()
dfFromRDD1.show()

val ids = Seq(1,2,3,4,5)

val scoreCol = ids.foldLeft(lit(null)) { case (acc, id) =>
  when(col("week_seq")===id, col(f"wk_$id%s_qty")).otherwise(acc)
  //when(col("week_seq")===id, col(fs"wk_$id")).otherwise(acc)
}

scoreCol

val df2 = dfFromRDD1.withColumn("week_value", scoreCol).select("fileCrtDt","week_value","week_seq")
df2.show()