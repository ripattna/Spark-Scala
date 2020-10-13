package com.demo

import org.apache.spark.sql.SparkSession

object Hive_Connection {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark Hive Example").master("local").getOrCreate()

    //Creating log level
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    spark.sql("create database if not exists temp_db")
    //spark.sql("create database if not exists demo")
    spark.sql("show databases")
    //spark.sql("use temp_db")
    //spark.sql("create table emp(id Int, salary Double) row format delimited  fields terminated by ',")
    //spark.sql("create table emp(id Int, salary Double)\n row format delimited \n  fields terminated by ',' \n  stored as textfile")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    // spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
    //spark.sql("show tables")
  }
}
