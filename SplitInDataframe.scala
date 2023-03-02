// Databricks notebook source
import org.apache.spark.sql.functions._

val data = Seq(("James, A, Smith","2018","M",3000),
    ("Michael, Rose, Jones","2010","M",4000),
    ("Robert,K,Williams","2010","M",4000),
    ("Maria,Anne,Jones","2005","F",4000),
    ("Jen,Mary,Brown","2010","",-1)
  )

import spark.sqlContext.implicits._
  val df = data.toDF("name","dob_year","gender","salary")
  df.printSchema()
  df.show(false)


val df2 = df.select(split(col("name"),",").getItem(0).as("FirstName"),
    split(col("name"),",").getItem(1).as("MiddleName"),
    split(col("name"),",").getItem(2).as("LastName"))
    .drop("name")

  df2.printSchema()
  df2.show(false)