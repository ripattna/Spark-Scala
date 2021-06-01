package com.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}


object SampleDataFrame extends App {

  // Create SparkSession
  val spark : SparkSession = SparkSession.builder().master("local[1]").appName("SampleDataFrame").getOrCreate()

  // Creating log level
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // val schemaDF = Seq("cust","acct","type","amt")

  val a = Seq(("abc","3","C",100),
    ("abc","3","C",100),
    ("xyz","4","C",500),
    ("abc","3","D",150),
    ("xyz","4","D",200),
    ("xyz","4","D",200)).toDF("cust","acct","type","amt")

  a.printSchema()
  a.show()

  val b = a.withColumn("credit",when(col("type")==="C",col("amt")).otherwise(0))
    .withColumn("debit",when(col("type")==="D",col("amt")).otherwise(0))

  b.show()

  val c = b.groupBy("Cust").sum("credit","debit").orderBy("cust")
    .select(col("cust"),(col("sum(credit)").as("credit")-col("sum(debit)").as("debit")).alias("total remaining amount"))

  c.show()
}
