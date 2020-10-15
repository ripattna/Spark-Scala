package com.rdd

import org.apache.spark.sql.SparkSession


object DATAtoDF {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder().appName("RDDTransformation").master("local").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    // Creating a sample dataset using list
    //val data = List((1,"Ram"),(2,"Sam"),(3,"Hari"),(4,"Rabi"))
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    import spark.implicits._
    // Create DataFrame from the data
    val dfFromData1 = data.toDF("id","Name")
    println("Creating DataFrame from DATA using toDF:")
    dfFromData1.printSchema()
    dfFromData1.show()
  }

}
