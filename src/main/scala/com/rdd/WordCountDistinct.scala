package com.rdd

import org.apache.spark.{SparkConf, SparkContext}

object WordCountDistinct {

  def main(args: Array[String]): Unit = {

    // Creating SparkConf and initializing SparkContext
    val conf = new SparkConf().setMaster("local").setAppName("Spark-Scala")
    val sc = new SparkContext(conf)

    // Creating log level
    sc.setLogLevel("ERROR")

    // Reading the text file
    val readRDD = sc.textFile("src/main/resources/Input.txt")

    // Spitting the input base on space
    val countRDD = readRDD.flatMap(x => x.split(" ")).map(x => (x, 1))

    // Applying reduceByKey to filter out the duplicate keys
    val finalRDD = countRDD.reduceByKey(_ + _)

    countRDD.take(num = 10).foreach(println)
    println("The countRDD:" + countRDD.foreach(println))

    println("Total word count:" + countRDD.count())
    println("Total distinct word count:" + finalRDD.count())

  }
}
