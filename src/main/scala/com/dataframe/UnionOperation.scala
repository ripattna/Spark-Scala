package com.dataframe

import org.apache.spark.sql.SparkSession

object UnionOperation {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("UnionOperation")
      .getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    // Step-1: Reading the first dataset
    val northDF = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("path", "src/main/resources/northern.csv")
      .load()
      .toDF("state", "capital")

    // Step-2: Show the 1st loaded input file
    northDF.show()

    // Step-3: Load the 2nd input file
    val westDF = spark.read
      .format("csv")
      .option("sep", ",")
      .option("path", "src/main/resources/western.csv")
      .load()
      .toDF("state", "capital")

    // Step-4: Show the 2nd loaded input file
    westDF.show()

    // Step-5: Combining both dataframes, sorting & showing results
    northDF
      .union(westDF)
      .sort(("state"))
      .show(false)
  }
}
