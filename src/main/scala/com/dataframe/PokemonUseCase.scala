package com.dataframe

import org.apache.spark.sql.SparkSession

object PokemonUseCase {
  def main(args: Array[String]): Unit = {

    // Creating SparkSession
    val spark = SparkSession.builder().master("local").appName("PokemonUseCase").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    // val path = "C:/Project/Files/Input/csv/Sample.csv"
    val df = spark.read.option("header", "true").csv("C:\\Users\\rissa\\Downloads\\PokemonData.csv")
    df.take(10).foreach(println)

    // df.createGlobalTempView("table")
    df.createOrReplaceTempView("records")
    val sqlDF = spark.sql("select * from records limit 10").show()

  }

}
