package com.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Order_Items_Data {
  def main(args: Array[String]): Unit = {

    //Create SparkConf Object
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)

    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("Akka").setLevel(Level.ERROR)
    sc.setLogLevel("ERROR")

    val orderItems = sc.textFile("E:\\Study\\data\\retail_db\\order_items")

    val revenuePerOrder = orderItems.
      map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat)).
      reduceByKey(_ + _).map(oi => oi._1 + "," + oi._2)
    //revenuePerOrder.saveAsTextFile(args(2))
    revenuePerOrder.take(15).foreach(println)
  }
}
