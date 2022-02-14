package com.automationn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, monotonically_increasing_id}
import java.io.{FileNotFoundException, IOException}

object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    try {
      val sourceDF = spark.read.option("header", "true").option("inferSchema", "true")
        .format("csv").load("src/main/resources/sourceFile1.csv")
      return sourceDF
    }
    catch {
      case e: FileNotFoundException => println("FileNotFoundException occurred")
      case e: IOException => println("IOException occurred")
    }

/*
    val targetDF = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/targetFile1.csv")

    val sourceDFCount =  sourceDF.agg(count("*").as("Source_Count")).withColumn("Column_Name", monotonically_increasing_id())
    sourceDFCount.show()

    val targetDFCount =  sourceDF.agg(count("*").as("Target_Count")).withColumn("Column_Name", monotonically_increasing_id())
    targetDFCount.show()

    val resultDF = sourceDF.groupBy().agg(count("*")).withColumn("Column_Name", monotonically_increasing_id())
    resultDF.show()

 */

  }
}

