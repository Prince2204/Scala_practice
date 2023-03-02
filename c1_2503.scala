// Databricks notebook source
 case class rep_runDates(emp_id: String, emp_name: String, emp_add: String,emp_sal: Integer)
     // case class rep_runDates(usecase_name: String, latest_report_date: Date, run_dates: Set[Date])
val d1 = rep_runDates("123","Prince","AAA XXX",10000)
      //val r1 = spark.sparkContext.parallelize(Seq("123","Prince","AAA XXX",10000))
val r1 = spark.sparkContext.parallelize(Seq(d1))
val df2 = r1.toDF("emp_id","emp_name","emp_add","emp_sal")
df2.printSchema()
df2.show()
      
      //var updated_dates_df  = report_rundates_df.union(df2)
     // val df1 = spark.createDataFrame(r1, rep_runDates)
     // updated_dates_df.printSchema()  
      
     // updated_dates_df.show 

// COMMAND ----------

//case class rep_runDates(emp_id: String, emp_name: String, emp_add: String,emp_sal: Integer)
val df1= Seq(("2019-10-23", "stbl" , "b"))

val r1 = spark.sparkContext.parallelize(Seq(df1))
val df2 = r1.toDF("emp_id","emp_name","emp_add")
df2.show

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class rep_runDates(dec_val: Double)
val d1 = rep_runDates(14.468)
val r1 = spark.sparkContext.parallelize(Seq(d1))
val df1 = r1.toDF("dec_val")
val df2 = df1.withColumn("dec_val",round(col("dec_val"),0))
df2.show
//val dec_num = 13.99
//println(round(lit(13.99),1))

// COMMAND ----------

