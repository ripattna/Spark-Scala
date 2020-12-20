package com.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PokemonUseCaseRDD {
  def main(args: Array[String]): Unit = {

    // Create SparkConf Object
    val conf = new SparkConf().setMaster("local").setAppName("PokemonUseCaseRDD")
    val sc = new SparkContext(conf)

    // Creating Spark Session
    val spark = SparkSession.builder().master("local").appName("Header_Footer_Removal").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")


    // Reading the dataset
    val PokemonSourceData  = spark.sparkContext.textFile("C:\\Project\\Files\\Input\\csv\\PokemonData.csv")

    println("#Get data Using collect:")
    // PokemonSourceData.collect().foreach(f => {println(f)})

    // Header record
    val header = PokemonSourceData.first()

    // Filtering the header
    val NoHeader = PokemonSourceData.filter(x => x != header)
    // val NoHeader1 = PokemonSourceData.filter(x => !x.contains(header))
    // val NoHeader2 = PokemonSourceData.filter(x => !x.equals(header))
    println("Data without header:")
    NoHeader.collect().foreach(f => {println(f)})

    // Getting the RDD side of noHeader RDD
    // println("Num of Partitions:" +NoHeader.partitions.size)
    println("Num of Partitions:" +NoHeader.getNumPartitions)

    // Filtering the watter pokemon
    val WatterRDD = PokemonSourceData.filter(x => x.contains("Water"))
    WatterRDD.collect().foreach(println)
    WatterRDD.count()

    // Filtering the Fire type pokemon
    val FireRDD = PokemonSourceData.filter(x => x.contains("Fire"))
    FireRDD.collect().foreach(println)
    FireRDD.count()

    val defenceList = NoHeader.map{x => x.split(',')}.map{x => (x(6).toDouble)}
    println("highest_defence:" +defenceList.max())
    println("lowest_defence:" +defenceList.min())

    // Max defence
    val defWithPokemonName1 = NoHeader.map{x => x.split(',')}.map{x => (x(6).toDouble, x(1))}
    val MaxDefencePokemon = defWithPokemonName1.groupByKey.takeOrdered(1)(Ordering[Double].reverse.on(_._1))
    println("Maximum defence:")
    MaxDefencePokemon.foreach(println)

    // Min defence Point
    val MinDefencePokemonPoint = defenceList.distinct.sortBy(x => x.toDouble, true,1)
    MinDefencePokemonPoint.take(5).foreach(println)

    // Min defence
    val defWithPokemonName2 = NoHeader.map{x => x.split(',')}.map{x => (x(6).toDouble, x(1))}
    val MinDefencePokemon = defWithPokemonName2.groupByKey.takeOrdered(1)(Ordering[Double].on(_._1))
    println("Minimum defence:")
    MinDefencePokemon.foreach(println)

  }

}
