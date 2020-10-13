package com.demo

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    //Creating log level
    import org.apache.log4j._
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    sc.setLogLevel("ERROR")

    // Reading the text file
    val readRDD = sc.textFile("C:\\Project\\Files\\Input\\text\\Input.txt")

    val countRDD = readRDD.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)

    println("The word count is:" + countRDD.count())

    // Saving the output file
   // countRDD.saveAsTextFile("C:\\Project\\Files\\Output\\WordCount_Results")

  }
}
