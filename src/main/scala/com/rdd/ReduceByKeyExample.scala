package com.rdd

import org.apache.spark.sql.SparkSession

object ReduceByKeyExample extends App {

  // Spark Session
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("Spark-Scala")
    .getOrCreate()

  // Creating log level
  spark.sparkContext.setLogLevel("WARN")

  val data = Seq((1, 2), (3, 4), (3, 6))

  val rdd = spark.sparkContext.parallelize(data)

  val rdd2 = rdd.reduceByKey(_ + _)

  rdd2.foreach(println)

}
