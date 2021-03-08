package com.dataframe

import org.apache.spark.sql.SparkSession

object CustomerInfo {

  def main(args: Array[String]): Unit = {

    //Creating SparkSession
    val spark = SparkSession.builder().appName("CustomerInfo").master("local").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    //Reading the file as csv
    val customerInfo = spark.read
      .option("sep", "|")
      .option("header", "false")
      .option("inferSchema", "true")
      .schema("Name String,Address String,City String,Country String,PinCode Int")
      .csv("C:\\Project\\Files\\Input\\text\\PipeDelimited.txt")
    customerInfo.collect().foreach(println)
    customerInfo.printSchema()

    customerInfo.createOrReplaceTempView("records")
    val sqlDF = spark.sql("select * from records limit 10").show(false)

    println("The Global temp view output")
    customerInfo.createOrReplaceGlobalTempView("sample")
    val sqlDF_Global = spark.sql("select * from global_temp.sample limit 10").show(true)

  }

}
