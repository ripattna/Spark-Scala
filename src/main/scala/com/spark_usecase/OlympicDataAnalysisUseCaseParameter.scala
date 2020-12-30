package com.spark_usecase

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OlympicDataAnalysisUseCaseParameter {

  def main(args: Array[String]): Unit = {

    if(args.length==3) {
    //Create Conf Object and to initializing the SparkContext
    //val conf = new SparkConf().setMaster("local").setAppName("OlympicData")
    //val sc = new SparkContext(conf)

    // Creating SparkSession
    val spark = SparkSession.builder()
      .master(args(0))
      .appName("OlympicData")
      .getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    val textFile = spark.sparkContext.textFile(args(1))
    val counts =  textFile.filter{
      x => {
        if(x.toString().split("\t").length >=10)
          true
        else
          false
      }
    }.map(line=>{line.toString().split("\t")})
    //counts.take(10).foreach(println)

    val fil = counts.filter(
      x => {
        if(x(5).equalsIgnoreCase("swimming")&&(x(9).matches(("\\d+"))))
          true
        else
          false
      }
    )
    //println(fil.count())

    val pairs: RDD[(String,Int)] = fil.map(x => (x(2),x(9).toInt))
    val cnt = pairs.reduceByKey(_+_).saveAsTextFile(args(2))
    //cnt.take(20).foreach(println)

     }
    else
      {
        println("Enter the three arguments")
      }

  }
}
