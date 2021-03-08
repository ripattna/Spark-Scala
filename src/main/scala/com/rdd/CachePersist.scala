package com.rdd

import org.apache.spark.sql.SparkSession

object CachePersist {

  def main(args: Array[String]): Unit = {

    //Creating SparkSession
    val spark = SparkSession.builder().appName("CachePersist").master("local").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    // Creating a sample dataset
    //val columns = Seq("ID","Text")
    val data = Seq(("1", "Be the change that you wish to see in the world"),
                   ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
                   ("3", "The purpose of our lives is to be happy."))

    //val df = data.toDF(columns:_*)
    val df = data.toDF("ID","Text")

    val dfCache = df.cache()
    dfCache.show(false)

  }
}
