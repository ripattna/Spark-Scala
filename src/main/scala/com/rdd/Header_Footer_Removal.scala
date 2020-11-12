package com.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Header_Footer_Removal {
  def main(args: Array[String]): Unit = {

    // Creating SparkContext and initializing Spark conf
    //val conf = new SparkConf().setMaster("local").setAppName("Header_Footer_Removal")
    //val sc = new SparkContext(conf)

    //Creating Spark Session
    val spark = SparkSession.builder().master("local").appName("Header_Footer_Removal").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    val rddFromFile  = spark.sparkContext.textFile("C:\\Project\\Files\\Input\\text\\HeaderFooter_Removal.txt")

    println("#Get data Using collect:")
    rddFromFile.collect().foreach(f => {println(f)})

    //Header record
    val header = rddFromFile.first()

    // Filtering the header
    val withoutHeader = rddFromFile.filter(x => !x.contains(header))
    println("Data without header:")
    withoutHeader.collect().foreach(f => {println(f)})

    val dataWithIndex = withoutHeader.zipWithIndex
    println("Print the data with the Index:")
    dataWithIndex.foreach(println)

    //Count of the records
    val count =  withoutHeader.count()
    println(count)

    //Filtering the footer records
    val cleansedData = dataWithIndex.filter(x => x._2 < count -1)
    println("Data without header & footer:")
    cleansedData.foreach(println)

  }

}
