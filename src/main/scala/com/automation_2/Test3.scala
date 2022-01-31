package com.automation_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}

object Test3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test3").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/File_1.csv")
    df1.show()
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/File_2.csv")
    df2.show()

    val primaryKeyList = Seq("ID")
    val schemaSchemaList = df1.columns.toList
    val columnToSelect = schemaSchemaList diff primaryKeyList

    //val joinResult = df1.as("df1").join(df2.as("df2"), Seq(primaryKeyList), "inner")
    // val leftResult = df1.as("df1").join(df2.as("df2"), Seq("primaryKeyList"), "left")
    // val rightResult = df1.as("df1").join(df2.as("df2"), Seq("primaryKeyList"), "right")

    val innerRes = df1.as("df1").join(df2.as("df2"), Seq("ID","Product", "Country","Quantity"), "inner")
    val leftRes = df1.as("df1").join(df2.as("df2"), Seq("ID","Product", "Country","Quantity"), "left")
    val rightRes = df1.as("df1").join(df2.as("df2"), Seq("ID","Product", "Country","Quantity"), "right")
    innerRes.show()
    leftRes.show()
    rightRes.show()
  }
}
