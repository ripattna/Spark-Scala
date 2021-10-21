package com.rdd

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    // Creating SparkConf and initializing SparkContext
    val conf = new SparkConf().setAppName("Spark-Scala").setMaster("local")
    val sc = new SparkContext(conf)

    //Creating log level
    sc.setLogLevel("ERROR")

    // Reading the text file
    val readRDD = sc.textFile("src/main/resources/Input.txt")

    val countRDD = readRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    countRDD.collect()

    println("The word count is:" + countRDD.count())

  }
}
