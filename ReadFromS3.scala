/**Required Dependencies:
* <dependency>
*           <groupId>org.apache.hadoop</groupId>
*            <artifactId>hadoop-common</artifactId>
*            <version>3.0.0</version>
*        </dependency>
*
*        <dependency>
*            <groupId>org.apache.hadoop</groupId>
*            <artifactId>hadoop-client</artifactId>
*            <version>3.0.0</version>
*        </dependency>
*        <dependency>
*            <groupId>org.apache.hadoop</groupId>
*            <artifactId>hadoop-aws</artifactId>
*            <version>3.0.0</version>
*        </dependency>
*/
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadTextFiles extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
 // Replace Key with your AWS account key (You can find this on IAM 
spark.sparkContext
     .hadoopConfiguration.set("fs.s3a.access.key", "awsaccesskey value")
service)
 // Replace Key with your AWS secret key (You can find this on IAM 
spark.sparkContext
     .hadoopConfiguration.set("fs.s3a.secret.key", "aws secretkey value")
spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
  spark.sparkContext.setLogLevel("ERROR")

  println("##spark read text files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile("s3a://sparkbyexamples/csv/text01.txt")
  println(rddFromFile.getClass)

  println("##Get data Using collect")
  rddFromFile.collect().foreach(f=>{
    println(f)
  })

  println("##read multiple text files into a RDD")
  val rdd4 = spark.sparkContext.textFile("s3a://sparkbyexamples/csv/text01.txt," +
    "s3a://sparkbyexamples/csv/text02.txt")
  rdd4.foreach(f=>{
    println(f)
  })

  println("##read text files base on wildcard character")
  val rdd3 = spark.sparkContext.textFile("s3a://sparkbyexamples/csv/text*.txt")
  rdd3.foreach(f=>{
    println(f)
  })

  println("##read all text files from a directory to single RDD")
  val rdd2 = spark.sparkContext.textFile("s3a://sparkbyexamples/csv/*")
  rdd2.foreach(f=>{
    println(f)
  })

  println("##read whole text files")
  val rddWhole:RDD[(String,String)] = spark.sparkContext.wholeTextFiles("s3a://sparkbyexamples/csv/text01.txt")
  println(rddWhole.getClass)
  rddWhole.foreach(f=>{
    println(f._1+"=>"+f._2)
  })
}
