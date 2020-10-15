package com.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {

  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    //Creating log level
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
    sc.setLogLevel("ERROR")

    //RDD can be created in 3 ways below are the example:
    //Creating a RDD by parallelize a collection
    val firstRDD = sc.parallelize(List("Spark", "Hadoop", "Kafka"))
    firstRDD.collect().foreach(println)

    //firstRDD.take(10).foreach(println)
    println("The number word in the RDD is: " + firstRDD.count())

    //Creating a RDD referencing a data set from a external storage system
    val readRDD = sc.textFile(path = "C:\\Project\\Files\\Input\\text\\Input.txt")
    readRDD.collect().foreach(println)

    //Creating a RDD by transforming a existing RDD
    val newRDD = readRDD.filter(x => x.contains("spark"))
    newRDD.collect().foreach(println)

    //Creating a RDD from existing DataFrame to DataSet
    val myRdd2 = spark.range(20).toDF().rdd
    myRdd2.collect().foreach(println)

    sc.stop()
  }
}
