package com.demo

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
object HdfsOperation {
  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf = new SparkConf().setAppName("HDFSOperation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //Set the log level to print the error
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    sc.setLogLevel("ERROR")

    println("Hello World")

  }
}
