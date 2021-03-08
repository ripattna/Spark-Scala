package com.rdd

import SparkRDD.dataSeq
import org.apache.spark.sql.SparkSession

object CreateEmptyRDD extends App {

  // Creating SparkSession
  val spark: SparkSession = SparkSession.builder().master("local[3]").appName("CreateEmptyRDD").getOrCreate()
  // Setting log level to error
  spark.sparkContext.setLogLevel("ERROR")

  // Creating Empty RDD without partition
  val rdd = spark.sparkContext.emptyRDD // creates EmptyRDD[0]
  println(rdd)
  println("Num of Partitions: " + rdd.getNumPartitions) // Returns o partition

  val rddString = spark.sparkContext.emptyRDD[String] // creates EmptyRDD[1]
  println(rddString)
  println("Num of Partitions: " + rddString.getNumPartitions) // returns o partition
  //rddString.saveAsTextFile("test.txt")

  // Creating Empty RDD with partition
  val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
  println(rdd2)
  println("Num of Partitions  : " + rdd2.getNumPartitions) //Outputs: initial partition count:3

  val rdd3 = spark.sparkContext.parallelize(dataSeq, 10)

  val new_rddPart = spark.sparkContext.parallelize(Seq(""))
  println(new_rddPart)
  println("Number of Partitions is:" + new_rddPart.getNumPartitions)

  // Pair RDD
  type dataType = (String, Int)
  var pairRDD = spark.sparkContext.emptyRDD[dataType]
  println(pairRDD)
  println("Number of Partitions of Pair RDD:" + pairRDD.getNumPartitions)

}
