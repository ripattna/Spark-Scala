package com.automation_2

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit, struct, sum, when}

object Test {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("ReconAutomation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("C:\\Project\\Files\\Input\\File_1.csv")
    df1.show()

    val df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("C:\\Project\\Files\\Input\\File_2.csv")
    df2.show()

    val schemaSchemaList = df1.columns.toList
    //println(schemaSchemaList)

    val result = df1.join(df2, df1.col("ID") === df2.col("ID"))
      .withColumn("M_Product", when(df1.col("Product") === df2.col("Product"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Country", when(df1.col("Country") === df2.col("Country"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Quantity", when(df1.col("Quantity") === df2.col("Quantity"), lit("Y")).otherwise(lit("N")))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
  result.show()

    println("Matching Records:")
    val diffDF = df1.intersect(df2)
    diffDF.show()

    println("Present in Source not in Target:")
    val inZ1 = df1.exceptAll(df2).toDF()
    inZ1.show()

    println("Present in Source not in Target:")
    val inZ = df1.except(df2)
    inZ.show()

    println("Present in Target not in Source:")
    val inZ2  = df2.exceptAll(df1).toDF()
    inZ2.show()

  }
}
