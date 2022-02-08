package com.mysql

import org.apache.spark.sql.SparkSession

object mysqlConnect {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("mysqlConnect").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val database = "demo"
    val table = "employee"
    val user = "root"
    val password  = "root"
    val connString = "jdbc:mysql://localhost:3306/"+database

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", connString)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()

    jdbcDF.show()

  }

}
