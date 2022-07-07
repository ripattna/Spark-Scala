package com.mysql

import org.apache.spark.sql.SparkSession

object mysqlConnect {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("mysqlConnect").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Database property
    val database = "demo"
    val table = "employee"
    val user = "rissan"
    val password = "rissan"
    val connString = "jdbc:mysql://localhost:3306/" + database

    // Connecting to mysql using JDBC connection
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", connString)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()
    jdbcDF.show(5)

    // Writing the Dataframe to HIVE table
    jdbcDF.write.saveAsTable("default.employee")

    // Querying in HIVE
    spark.sql("select * from default.employee").show()

  }
}
