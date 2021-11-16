package com.rdd

import org.apache.spark.sql.SparkSession

object toDF {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder().appName("Spark-Scala").master("local").getOrCreate()

    // Creating log level
    // spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    // Creating a sample Dataset
    val columns = Seq("id","Name")
    val data = List((1,"Ram"),(2,"Sam"),(3,"Hari"),(4,"Rabi"))

    // Create DataFrame from the data
    import spark.implicits._
    // val dfFromData1 = data.toDF("id","Name")
    val dfFromData = data.toDF(columns:_*)
    println("Creating DataFrame from DATA using toDF:")
    dfFromData.printSchema()
    dfFromData.show()

    // Create DataFrame from the RDD
    val rdd = spark.sparkContext.parallelize(data)
    val dfFromRDD = rdd.toDF(columns:_*)
    println("Creating DataFrame from RDD using toDF:")
    dfFromRDD.printSchema()
    dfFromRDD.show()

  }
}
