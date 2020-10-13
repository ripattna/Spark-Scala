package com.demo

import org.apache.spark.sql.SparkSession

object Csv_Operation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Hive Example")
      .master("local[1]")
      .getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    val path ="C:/Project/Files/Input/csv/Sample.csv"
    val df = spark.read.option("header","true").csv(path)
    // df.take(10).foreach(println)

    df.printSchema()
    // df.show()

    // df.createGlobalTempView("table")
    df.createOrReplaceTempView("records")
    val sqlDF= spark.sql("select * from records limit 10").show()

  }

}
