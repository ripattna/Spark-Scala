package com.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ChargeTest {

  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf = new SparkConf().setMaster("local").setAppName("ChargeTest")
    // val sc = new SparkContext(conf)
    val sc = SparkContext.getOrCreate(conf)

    // Reading the text file
    val customerDetails = sc.textFile("C:\\Project\\Files\\Input\\Telkomsel_Feed")
    customerDetails.take(5).foreach(println)

    // Replacing the delimiter
    val myRDD = customerDetails.map(x => x.replace("|", "#"))
    println("Below the modified value:")
    myRDD.collect().foreach(println)
    myRDD.saveAsTextFile("C:\\Project\\Files\\Output\\Telkomsel_Feed")

  }
}
