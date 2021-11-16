package com.rdd

import org.apache.spark.sql.SparkSession

object RDDRepartitionExample extends App {

  // Creating SparkSession
  val spark: SparkSession = SparkSession.builder().master("local[5]").appName("RDD_Repartition").getOrCreate()

  val rdd1 = spark.sparkContext.parallelize(Range(0, 20), 6)
  println("parallelize : " + rdd1.partitions.size)
  rdd1.partitions.foreach(f => f.toString)

  val rddFromFile = spark.sparkContext.textFile("src/main/resources/Input.txt", 9)
  rddFromFile.collect().foreach(println)
  println("TextFile : " + rddFromFile.partitions.size)
  // rdd1.saveAsTextFile("c:/tmp/partition")

  val rdd2 = rdd1.repartition(4)
  println("Repartition size : " + rdd2.partitions.size)
  // rdd2.saveAsTextFile("c:/tmp/re-partition1")

  val rdd3 = rdd1.coalesce(4)
  println("Repartition size : " + rdd3.partitions.size)
  // rdd3.saveAsTextFile("c:/tmp/coalesce")

}
