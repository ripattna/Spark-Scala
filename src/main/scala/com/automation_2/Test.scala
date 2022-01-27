package com.automation_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, sum, when}

object Test {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("ReconAutomation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("C:\\Project\\Files\\Input\\File_1.csv")
    df1.show()

    val schemaSchemaList = df1.columns.toList
    println(schemaSchemaList)

    val df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("C:\\Project\\Files\\Input\\File_2.csv")
    df2.show()

    val result = df1.join(df2, df1.col("ID") === df2.col("ID"))
      .withColumn("M_Product", when(df1.col("Product") === df2.col("Product"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Country", when(df1.col("Country") === df2.col("Country"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Quantity", when(df1.col("Quantity") === df2.col("Quantity"), lit("Y")).otherwise(lit("N")))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))

  result.show()

  }

}
