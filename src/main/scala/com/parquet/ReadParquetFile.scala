package com.parquet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

object ReadParquetFile {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("ParquetSample").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val readParquet = spark.read.format("parquet").load("src/main/resources/file1.parquet")

    val df = spark.read.parquet("src/main/resources/file1.parquet")
    df.cache()
    // df.show(false)
    df.withColumn("file_name", input_file_name()).select("file_name").distinct().show()

  }

}
