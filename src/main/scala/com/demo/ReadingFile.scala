package com.demo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object ReadingFile {
 def main(args:Array[String]): Unit={

   // Create Conf Object and to initializing the SparkContext
   val conf=new SparkConf().setMaster("local").setAppName("ReadingFile")
   val sc=new SparkContext(conf)

   // Creating log level
   Logger.getLogger("org").setLevel(Level.ERROR)
   Logger.getLogger("akka").setLevel(Level.ERROR)
   sc.setLogLevel("ERROR")

   val ReadRDD= sc.textFile(path ="C:\\Temp\\Input\\Sample.txt")
  // ReadRDD.take(num = 10).foreach(println)
  // println(ReadRDD.foreach(println))
   println("The count of RDD is:",ReadRDD.count())

 }
}
