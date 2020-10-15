package com.spark_usecase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object OlympicDataAnalysis {

  def main(args: Array[String]): Unit = {


    // Create SparkSession
    val spark = SparkSession.builder().appName("OlympicDataAnalysis").master("local").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    // Reading the csv file
    val df = spark.read
      .option("delimiter",",")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .schema("Name string ,Age integer ,Country string ,Year string ,Closing_Date string ,Sport string ,Gold_Medals integer ,Silver_Medals integer ,Bronze_Medals integer ,Total_Medals integer")
      .csv("C:\\Project\\Files\\Input\\csv\\olympics_data.csv")

    df.createGlobalTempView("olympic")
    val sqlDF = spark.sql("select * from global_temp.olympic limit 5").show()

  }

}