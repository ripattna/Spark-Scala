package com.rdd

import org.apache.spark.{SparkConf, SparkContext}

object FirstSparkProgram {

  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf = new SparkConf().setMaster("local").setAppName("FirstSparkProgram")
    val sc = new SparkContext(conf)

    // Creating log level
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    sc.setLogLevel("ERROR")

    // Creating the RDD
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6))
    rdd1.collect().foreach(println)
    println("Number of element in RDD:", rdd1.count())
  }
}
