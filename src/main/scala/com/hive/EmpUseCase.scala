package com.hive

import org.apache.spark.sql.SparkSession

object EmpUseCase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("EmpUseCase").master("local").enableHiveSupport().getOrCreate()

    // Creating log level
    //spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    // Creating database demo
    spark.sql("create database if not exists demo")

    // List the databases
    spark.sql("show databases").show()

    // Setting the current database to Demo Database
    spark.catalog.setCurrentDatabase("demo")

    // Creating the emp table
    spark.sql(
      "CREATE TABLE IF NOT EXISTS emp(Id INT, Name STRING,Gender STRING,Salary STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

    spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/Employee.txt' INTO TABLE demo.emp")
    spark.sql("select * from demo.emp").show()

  }

}
