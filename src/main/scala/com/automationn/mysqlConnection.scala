package com.automationn

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, count, monotonically_increasing_id}
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

  /**
   * Will return the dataframe record count
   * @param sourceDF record count
   * @param targetDF record count
   * @return  DataFrame
   */
  def totalRecordCount(sourceDF: DataFrame, targetDF: DataFrame, alias: String): DataFrame = {

    try {
      // Make sure that column names match in both DataFrames
      if (sourceDF.schema != targetDF.schema)
      {
        print("Column schema are different in source and target!")
        throw new Exception("Column schema Did Not Match")
      }
      // Make sure that schema of both DataFrames are same
      else if (!sourceDF.columns.sameElements(targetDF.columns))
      {
        println("Column names anc count were different in source and target!!!")
        throw new Exception("Column count and column name Did Not Match")
      }
    }

    catch {
      case ex: Exception => println(s"Found a unknown exception: $ex")
        System.exit(0)
      case e: AnalysisException => println(e)
    }

    try{
      sourceDF.agg(count("*").as(alias))
        .withColumn("Column_Name", monotonically_increasing_id())
    }
  }

  /**
   * Will join source and target dataframe inorder to get the extra records in source and target
   * @param joinType type of the join(left_anti)
   * @param sourceDF sourceDF
   * @param targetDF targetDF
   * @param schemaSchemaList
   * @return  DataFrame
   */
  def joinDF(sourceDF: DataFrame,targetDF: DataFrame, schemaSchemaList: List[String],
             joinType: String, alias: String): DataFrame = {

     sourceDF.na.fill(0).join(targetDF.na.fill(0), schemaSchemaList, joinType)
       .agg(count("*").as(alias))
       .withColumn("Column_Name", monotonically_increasing_id())
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

    /**
     * Now calling the method which is available in mysqlConnection class
     * Now calling the method which is available in mysqlConnection class
     * Now calling the method which is available in mysqlConnection class
     **/

    val sourceDF = new mysqlConnection().readFile(readType, fileType, sourcePath, connString, driverName, database, table, user, password)
    println("Source Data:")
    sourceDF.show()

    val targetDF = new mysqlConnection().readFile(readType, fileType, targetPath, connString, driverName, database, table, user, password)
    println("Target Data:")
    targetDF.show()

    // Schema of Source Data in List
    val schemaSchemaList = sourceDF.columns.toList

    // Columns to select after ignoring Primary Key
    val columnToSelect = schemaSchemaList diff primaryKeyList

    val sourceRecCount = new mysqlConnection().totalRecordCount(sourceDF,targetDF, "Source_Rec_Count")
    val targetRecCount = new mysqlConnection().totalRecordCount(targetDF,sourceDF, "Target_Rec_Count")

    val overlapRecCount = new mysqlConnection().joinDF(sourceDF, targetDF, schemaSchemaList, "inner", "Overlap_Rec_Count")

    // Extra Records in Source
    val extraSourceRecCount = new mysqlConnection()
      .joinDF(sourceDF, targetDF, schemaSchemaList, "left_anti", "Source_Extra_Rec_Count")

    // Extra Records in Target
    val extraTargetRecCount = new mysqlConnection()
      .joinDF(targetDF, sourceDF, schemaSchemaList, "left_anti","Target_Extra_Rec_Count" )

    val joinResult = sourceRecCount
      .join(targetRecCount, Seq("Column_Name"),"inner")
      .join(overlapRecCount, Seq("Column_Name"),"inner")
      .join(extraSourceRecCount, Seq("Column_Name"),"inner")
      .join(extraTargetRecCount, Seq("Column_Name"),"inner").drop("Column_Name")
    joinResult.show()

  }
}