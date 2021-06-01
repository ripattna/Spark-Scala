package com.rdd

import org.apache.spark.sql.SparkSession


object Broadcast_Variable {

  def main(args: Array[String]): Unit = {

    // Creating SparkSession
    val spark = SparkSession.builder().appName("BroadCast_Variable").master("local").getOrCreate()

    val ep = Seq(("Harry",1),("Soma",1),("Sandy"),3,("Andy",2),("Ted",3))

    val emp = spark.sparkContext.parallelize(ep)

    emp.collect().foreach(println)

    // Creating a HashMap
    val dp =Map(1-> "a", 2-> "b", 3->"c")

    val dep = spark.sparkContext.broadcast(dp)

    //emp.map(x => x._1 +" ,"+ x._2 +" ," + dep+value.get(x._2).get).collect().foreach(println)

  }

}
