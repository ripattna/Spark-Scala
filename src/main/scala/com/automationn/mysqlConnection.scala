package com.automationn

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


class mysqlConnection {

  // Spark Session
  val spark = SparkSession.builder()
    .master("local")
    .appName("ReconTest")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  /**
   * Read two source and target files, whether in S3 , HDFS , local file system
   * For example, for HDFS, "hdfs://nn1home:8020/input/war-peace.parquet"
   * For S3 location, "s3n://myBucket/myFile1.csv"
   * @param fileType could be parquet,csv,json  etc
   * @param filePath where the file reside in any of the storage
   * @return  DataFrame
   */
  def readFile(readType: String, fileType: String, filePath: String, connString: String, driverName: String,
               database: String, table: String, user: String, password: String): DataFrame = {

    if (readType == "file") {
      try {
        spark.read.option("header", "true").option("inferSchema", "true").format(fileType).load(filePath)
      }
    }

    else if (readType == "database") {
      try {
      spark.read.format("jdbc").option("url", connString).option("driver", driverName).option("dbtable", table)
        .option("user", user).option("password", password).load()
      }

    }

    else {
      spark.read.format("").load()
    }
  }

}

object mysqlConnectionObject {

  def main(args: Array[String]): Unit = {

    // Reading the conf file
    val config: Config = ConfigFactory.load("config.conf")

    // Reading the file format from config
    val readType: String = config.getString("readType")

    // Reading the source and target file from config
    val sourcePath: String = config.getString("fileDetails.sourceFile")
    val targetPath: String = config.getString("fileDetails.targetFile")

    val fileType: String = config.getString("fileDetails.fileType")

    // Reading the database connection
    val database : String = config.getString("databaseDetails.databaseName")
    val table : String = config.getString("databaseDetails.tableName")
    val user = config.getString("databaseDetails.userName")
    val password =  config.getString("databaseDetails.password")
    val driverName = config.getString("databaseDetails.driverName")
    val connString = config.getString("databaseDetails.connString")

    // Reading the PrimaryKey from config
    val primaryKeyList = config.getStringList("primaryKey.primaryKeyValue").toList

    val sourceDF = new mysqlConnection().readFile(readType, fileType, sourcePath, connString, driverName, database, table, user, password)
    println("Source Data:")
    sourceDF.show()
    val targetDF = new mysqlConnection().readFile(readType, fileType, targetPath, connString, driverName, database, table, user, password)
    println("Target Data:")
    targetDF.show()

  }
}