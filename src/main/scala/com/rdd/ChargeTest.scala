package com.rdd

import org.apache.spark.{SparkConf, SparkContext}

object ChargeTest {

  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf = new SparkConf().setMaster("local").setAppName("ChargeTest")
    val sc = SparkContext.getOrCreate(conf)
    // val sc = new SparkContext(conf)

    // Setting the log level to Error
     sc.setLogLevel("ERROR")

    // Reading the text file
    val customerDetails = sc.textFile("src/main/resources/Employee.txt")
    customerDetails.take(5).foreach(println)

    // Replacing the delimiter
    val myRDD = customerDetails.map(x => x.replace(",", "|"))
    println("Below the modified value:")
    myRDD.collect().foreach(println)

  }
}
