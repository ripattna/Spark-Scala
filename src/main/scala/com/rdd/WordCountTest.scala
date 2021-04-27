package com.rdd

import org.apache.spark.{SparkConf, SparkContext}

object WordCountTest {

  def main(args: Array[String]): Unit = {

    // Creating SparkConf and initializing SparkContext
    val conf = new SparkConf().setMaster("local").setAppName("WordCountTest")
    val sc = new SparkContext(conf)

    // Creating log level
    sc.setLogLevel("ERROR")

    // Reading the text file
    val readRDD = sc.textFile("C:\\Project\\Files\\Input\\text\\WordCountInput.txt")

    // Spitting the input base on space
    val countRDD = readRDD.flatMap(x => x.split(" ")).map(x => (x, 1))

    // Applying reduceByKey to filter out the duplicate keys
    val finalRDD = countRDD.reduceByKey(_ + _)

    // countRDD.take(num = 10).foreach(println)
    // println("The countRDD:" + countRDD.foreach(println))

    println("Total word count:" + countRDD.count())
    println("Total distinct word count:" + finalRDD.count())

    countRDD.saveAsTextFile("C:\\Project\\Files\\Output\\WordCount")
    finalRDD.saveAsTextFile("C:\\Project\\Files\\Output\\WordCountFinal")

  }
}
