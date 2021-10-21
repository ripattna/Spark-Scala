package com.rdd

import org.apache.spark.{SparkConf, SparkContext}

object ReadingFile {

  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf = new SparkConf().setMaster("local").setAppName("Spark-Scala")
    val sc = new SparkContext(conf)

    // Creating log level
    sc.setLogLevel("ERROR")

    // Reading the File
    val readRDD  = sc.textFile("src/main/resources/Input.txt")

    // Printing thr top 10 lines
    readRDD.take(num = 10).foreach(println)

    // Printing the records
    readRDD.foreach(println)

    // Printing the no of line count
    println("The count of RDD is:", readRDD.count())

  }
}
