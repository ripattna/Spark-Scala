package com.spark_usecase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OlympicDataAnalysisUseCase {

  def main(args: Array[String]): Unit = {

    // Creating SparkSession
    val spark = SparkSession.builder().appName("OlympicData").master("local").getOrCreate()
    // Creating log level
    spark.sparkContext.setLogLevel("WARN")
    // Reading the Olympics datasets
    val textFile = spark.sparkContext.textFile("src/main/resources/olympics_data.csv")
    val counts = textFile
      .filter { x =>
        {
          if (x.toString().split("\t").length >= 10)
            true
          else
            false
        }
      }
      .map(line => { line.toString().split("\t") })
    counts.take(10).foreach(println)

    val fil = counts.filter(
      x => {
        if (x(5).equalsIgnoreCase("swimming") && x(9).matches("\\d+"))
          true
        else
          false
      }
    )

    println(fil.count())
    val pairs: RDD[(String, Int)] = fil.map(x => (x(2), x(9).toInt))
    val cnt = pairs.reduceByKey(_ + _)
    cnt.take(20).foreach(println)

  }

}
