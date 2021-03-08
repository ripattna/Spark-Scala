package com.dataframe

import org.apache.spark.sql.SparkSession

object CustomerInfoTempGlobal {

  def main(args: Array[String]): Unit = {

    // Creating SparkSession and
    val spark = SparkSession.builder().master("local[1]").appName("CustomerInfoTempGlobal").getOrCreate()

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

    spark.stop()

  }

}
