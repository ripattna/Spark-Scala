package com.automation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().appName("New_Test").master("local").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val input = spark.sparkContext.parallelize(Seq(
      ("a", 5, 7, 9, 12, 13),
      ("b", 6, 4, 3, 20, 17),
      ("c", 4, 9, 4, 6 , 9),
      ("d", 1, 2, 6, 8 , 1)
    )).toDF("ID", "var1", "var2", "var3", "var4", "var5")

    val columnsToSum = List(col("var1"), col("var2"), col("var3"), col("var4"), col("var5"))

    val output = input.withColumn("sums", columnsToSum.reduce(_ + _))

    output.show()

  }
}
