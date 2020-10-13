package com.demo

import org.apache.spark.{SparkConf, SparkContext}

object GetRevenuePerOrder {
  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf=new SparkConf().setMaster(args(0)).setAppName("GetRevenuePerOrder")
    val sc =new SparkContext(conf)

    //Creating log level
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    sc.setLogLevel("ERROR")

    val orderItems=sc.textFile(args(1))
    val revenuePerOrder=orderItems.
      map(oi => (oi.split(",")(1).toInt,oi.split(",") (4).toFloat)).
      reduceByKey(_ + _).
      map(oi =>oi._1 + "," + oi._2)
    revenuePerOrder.saveAsTextFile(args(2))
  }
}
