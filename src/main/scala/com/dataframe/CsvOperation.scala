package com.dataframe

import org.apache.spark.sql.SparkSession

object CsvOperation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark Hive Example").master("local[1]").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    // val path = "src/main/resources/western.csv"
    // val df3 = spark.read.option("header", "true").csv(path)

    val df = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", true)
      .load("src/main/resources/northern.csv")

    df.take(10).foreach(println)

    df.printSchema()
    // df.show()

    // df.createGlobalTempView("table")
    df.createOrReplaceTempView("records")
    val sqlDF = spark.sql("select * from records limit 10").show()

  }

}
