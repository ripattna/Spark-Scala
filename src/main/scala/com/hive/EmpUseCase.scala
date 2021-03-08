package com.hive

import org.apache.spark.sql.SparkSession

object EmpUseCase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("EmpUseCase").master("local").getOrCreate()

    // Creating log level
    //spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    // Creating database demo
    spark.sql("create database if not exists demo")
    //List the databases
    spark.sql("show databases").show()
    // Setting the current database to Demo Database
    spark.catalog.setCurrentDatabase("demo")
    //spark.sql("use demo")
    //creating the emp table
    spark.sql("CREATE TABLE IF NOT EXISTS emp(id INT, name STRING, location STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

    spark.sql("LOAD DATA LOCAL INPATH '/C:/Project/Files/Input/emp.txt' INTO TABLE demo.emp")
    spark.sql("select * from demo.emp").show()

  }

}