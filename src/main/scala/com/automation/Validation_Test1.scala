package com.automation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Validation_Test1 {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("ERROR")

    // Read the source file
    val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Project\\Files\\df1.csv")
    println("Source Record:")
    df1.show()
    // Read the target file
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Project\\Files\\df2.csv")
    //df2.show()

    //println("The difference source and target:")
    //df2.except(df1).show(false)

    //df1.diff(df2, "Primary_Key").show(false)

    // Creating a TempView
    //df1.createOrReplaceTempView("records")
    // No of rows in source where value is 1
    //spark.sql("select sum(valueA) as valueA,sum(valueB) as valueB,sum(valueC) as valueC from records").show()
    // spark.sql("select count(*) as valueA from records where valueA=1").show()

    val new_df1 = df1.withColumn("sum_of_columns", df1("valueA"))
    new_df1.show()

    val columnsToSum = List(col("valueA"), col("valueB"), col("valueC"), col("valueD"))
    val output = df1.withColumn("sums", columnsToSum.reduce(_ + _))
    output.show()

  }

}
