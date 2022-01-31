package com.automation_2

import org.apache.spark.sql.SparkSession

object Test2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/File_1.csv")
    df1.show()
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/File_2.csv")
    df2.show()

    val columns = df1.columns
    columns.foreach(println)

    val columns1 = df1.schema.fields.map(_.name)
    columns.foreach(println)

    val selectiveDifferences = columns.map(col => df1.select(col).except(df2.select(col)))
    selectiveDifferences.map(diff => {if(diff.count > 0) diff.show})

  }

}
