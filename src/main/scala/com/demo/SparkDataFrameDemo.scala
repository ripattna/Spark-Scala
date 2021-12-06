package com.demo

import org.apache.spark.sql.SparkSession

object SparkDataFrameDemo {

  def main(args: Array[String]): Unit = {
    println("Hello Spark Scala")

    // Create a Spark Session
    val spark = SparkSession
      .builder
      .appName("HelloSpark")
      .config("spark.master", "local")
      .getOrCreate()
    println("Created Spark Session")

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    val sampleSeq = Seq((1, "spark"), (2, "Big Data"))
    val df = spark.createDataFrame(sampleSeq).toDF("Course_Id", "Course_Name")
    df.show()

  }

}
