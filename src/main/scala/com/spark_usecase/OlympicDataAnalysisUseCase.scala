package com.spark_usecase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object OlympicDataAnalysisUseCase {

  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    // val conf = new SparkConf().setMaster("local").setAppName("OlympicData")
    // val sc = new SparkContext(conf)

    // Creating SparkSession
    val spark = SparkSession.builder().appName("OlympicData").master("local").getOrCreate()
    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    val textFile = spark.sparkContext.textFile("C:\\Project\\Files\\Input\\olympic")
    val counts =  textFile.filter{
      x => {
        if(x.toString().split("\t").length >=10)
          true
        else
          false
      }
    }.map(line=>{line.toString().split("\t")})
   counts.take(10).foreach(println)
    val fil = counts.filter(
      x => {
        if(x(5).equalsIgnoreCase("swimming")&&(x(9).matches(("\\d+"))))
        true
      else
        false
      }
    )
    println(fil.count())
    val pairs: RDD[(String,Int)] = fil.map(x => (x(2),x(9).toInt))
    val cnt = pairs.reduceByKey(_+_)
    cnt.take(20).foreach(println)

  }

}
