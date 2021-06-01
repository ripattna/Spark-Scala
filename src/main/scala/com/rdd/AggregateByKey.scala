package com.rdd

import com.dataframe.SampleDataFrame.spark
import org.apache.spark.sql.SparkSession

object AggregateByKey extends App{

  //Create SparkSession
  val spark = SparkSession.builder().appName("AggregateByKey").master("local").getOrCreate()

  // Creating log level
  spark.sparkContext.setLogLevel("ERROR")

  val raw_date = Seq("Harry=78", "Raj=75","Harry=67","Raj=89","Raj=67","Raj=72","Harry=72")
  val data = spark.sparkContext.parallelize(raw_date)
  data.collect().foreach(println)
  val pairRDD = data.map(x => x.split("=")).map(x =>(x(0),x(1)))
  println("The pairRDD Value:")
  pairRDD.collect().foreach(println)

  val initial = 0
  val addOp = (sum:Int, new_value:String) => sum + new_value.toInt
  val mergeOp = (p1:Int ,p2: Int) => p1+p2
  val out = pairRDD.aggregateByKey(initial)(addOp, mergeOp)
  println("The Final Value:")
  out.collect.foreach(println)

}
