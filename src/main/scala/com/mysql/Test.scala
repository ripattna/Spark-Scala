package com.mysql

import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("mysqlConnection")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val dataframe_mysql = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/demo")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "covid_cases")
      .option("user", "root")
      .option("password", "root")
      .load()
    dataframe_mysql.show()

  }

}
