package com.rdd

import org.apache.spark.sql.SparkSession

object SparkRDD extends App {

  // Creating SparkSession
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark-Scala")
    .getOrCreate()

  // Creating log level
  spark.sparkContext.setLogLevel("ERROR")

  // Create RDD from parallelize
  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rdd1 = spark.sparkContext.parallelize(dataSeq)
  println("RDD using Parallelize")
  rdd1.collect().foreach(println)

  // Create RDD from external Data source
  val rdd2 = spark.sparkContext.textFile("src/main/resources/Input.txt")
  println("RDD using textFile")
  rdd2.collect().foreach(println)

  // Read entire file into a RDD as single record.
  val rdd3 = spark.sparkContext.wholeTextFiles("src/main/resources/Input.txt")
  println("RDD using wholeTextFiles")
  rdd3.collect().foreach(println)

}
