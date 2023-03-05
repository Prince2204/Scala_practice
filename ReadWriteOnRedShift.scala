/*
*  Dependency required
*  groupId: com.github.dnaumenko
*  artifactId: spark-redshift_2.11
*  version: 2.0.2
*/

import org.apache.spark.sql._

val sc = // existing SparkContext
val sqlContext = new SQLContext(sc)

// Get some data from a Redshift table
val df: DataFrame = sqlContext.read
    .format("com.databricks.spark.redshift")
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
    .option("dbtable", "my_table")
    .option("tempdir", "s3n://path/for/temp/data")
    .load()

// Can also load data from a Redshift query
val df: DataFrame = sqlContext.read
    .format("com.databricks.spark.redshift")
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
    .option("query", "select x, count(*) my_table group by x")
    .option("tempdir", "s3n://path/for/temp/data")
    .load()

// Apply some transformations to the data as per normal, then you can use the
// Data Source API to write the data back to another table

df.write
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
  .option("dbtable", "my_table_copy")
  .option("tempdir", "s3n://path/for/temp/data")
  .mode("error")
  .save()

// Using IAM Role based authentication
df.write
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
  .option("dbtable", "my_table_copy")
  .option("aws_iam_role", "arn:aws:iam::123456789000:role/redshift_iam_role")
  .option("tempdir", "s3n://path/for/temp/data")
  .mode("error")
  .save()
