package com.rdd

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SparkSession

object RddInterviewAskedExercise {

  def main(args: Array[String]): Unit = {

    // Creating SparkSession
    val spark = SparkSession.builder().appName("RddExercise").master("local").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    // Creating a sample dataset using list
    val data = List((1,"Ram"),(2,"Sam"),(3,"Hari"),(4,"Rabi"))
    // val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    // Converting list to RDD
    val newRDD = spark.sparkContext.parallelize(data, 3)
    // Printing the RDD
    newRDD.take(10).foreach(println)

    // Printing the RDD partition
    println("Partition Size:" +newRDD.partitions.size)
    println("No of Partitions:" +newRDD.getNumPartitions)

    // Converting the second element of the RDD to Uppercase
    val conv = newRDD.map(x => (x._2.toUpperCase()))
    println("Changing the name to Uppercase:\n")
    conv.take(10).foreach(println)

    // Creating DataFrame from the data using createDataFrame
    val dfFromData = spark.createDataFrame(data).toDF("Id","Name")
    dfFromData.printSchema()
    dfFromData.show()

    // Creating DataFrame from the data using createDataFrame
    import spark.implicits._
    val dfFromData1 = data.toDF("Id","Name")
    dfFromData1.printSchema()
    dfFromData1.show()

  }
}
