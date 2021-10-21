package com.rdd

import org.apache.spark.{SparkConf, SparkContext}

object  RDDTransformation {
  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf = new SparkConf().setMaster("local").setAppName("RDDTransformation")
    val sc = new SparkContext(conf)

    // Creating log level
    sc.setLogLevel("ERROR")

    val readRDD = sc.textFile(path = "src/main/resources/Input.txt")
    // readRDD.collect().foreach(println)
    val linesWithSpark = readRDD.filter(line => line.contains("Spark"))
    println("The lines where Spark present in the test:")
    linesWithSpark.collect().foreach(println)
    println("Number of times Spark present in the test:", linesWithSpark.count())
    //val newRDD = readRDD.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
    sc.stop()
  }
}
