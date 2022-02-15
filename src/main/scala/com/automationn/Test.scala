package com.automationn

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, monotonically_increasing_id, sum}

import java.io.{FileNotFoundException, IOException}

object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    try {
      val sourceDF = spark.read.option("header", "true").option("inferSchema", "true")
        .format("csv").load("src/main/resources/sourceFile1.csv")

    }
    catch {
      case e: FileNotFoundException => println("FileNotFoundException occurred")
      case e: IOException => println("IOException occurred")
    }

    val sourceDF = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/sourceFile.csv")
    sourceDF.show()
    val targetDF = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/targetFile.csv")
    targetDF.show()

    /*
    val sourceDFCount =  sourceDF.agg(count("*").as("Source_Count")).withColumn("Column_Name", monotonically_increasing_id())
    sourceDFCount.show()
    val targetDFCount =  targetDF.agg(count("*").as("Target_Count")).withColumn("Column_Name", monotonically_increasing_id())
    targetDFCount.show()
    val resultDF = sourceDF.groupBy().agg(count("*")).withColumn("Column_Name", monotonically_increasing_id())
    resultDF.show()


    val resSource = sourceDF
      .agg(count("Product").as("Product"),
        count("Country").as("Country"),
        count("Quantity").as("Quantity"))
      .withColumn("Index", monotonically_increasing_id())
    resSource.show()

     */

    val resTarget = targetDF
      .agg(
        count("Product").as("Product"),
        count("Country").as("Country"),
        count("Quantity").as("Quantity")
      )
      .withColumn("Index", monotonically_increasing_id())
    resTarget.show()

    //val expr = sourceDF.columns.map(_ -> "count").toMap
    //sourceDF.groupBy().agg(expr).show()

    val exprs = targetDF.columns.map(_ -> "count").toMap
    exprs.foreach(println)
    //sourceDF.groupBy().agg(exprs.head, exprs.tail: _*).show()
    targetDF.groupBy().agg(exprs).show()

    val newDF = targetDF.columns.map(i => (i , "count")).toMap
    print(newDF)
    newDF.foreach(println)
    val newCols = targetDF.groupBy().agg(newDF)
   // newDF.select(newCols: _*).show()
      .na.fill(0)
      .withColumn("Column_Name", monotonically_increasing_id())

  }
}

