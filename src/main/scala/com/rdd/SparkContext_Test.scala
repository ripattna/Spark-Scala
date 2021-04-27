package com.rdd

import com.rdd.CreateEmptyRDD.spark
import org.apache.spark.{SparkConf, SparkContext}

object SparkContext_Test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("a").setMaster("local")
    // val sc = new SparkContext(conf)
    val sc = SparkContext.getOrCreate(conf)

    // Setting log level to error
    sc.setLogLevel("ERROR")

    // Creating the RDD
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6))
    rdd1.collect().foreach(println)
    println("Number of element in RDD:", rdd1.count())

  }

}
