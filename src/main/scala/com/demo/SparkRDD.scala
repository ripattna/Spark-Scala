package com.demo

import org.apache.spark.sql.SparkSession

object SparkRDD extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkRDD")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //Create RDD from parallelize
  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rdd_new =spark.sparkContext.parallelize(dataSeq)

  //Create RDD from external Data source
  val rdd2 = spark.sparkContext.textFile("C:\\Project\\Files\\Input\\text\\Input.txt")

  //Reads entire file into a RDD as single record.
  val rdd3 = spark.sparkContext.wholeTextFiles("C:\\Project\\Files\\Input\\text\\Input.txt")
  //rdd3.collect().foreach(println)

}
