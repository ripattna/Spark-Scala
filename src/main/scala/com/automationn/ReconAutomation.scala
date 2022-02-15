package com.automationn

import java.io.{FileNotFoundException, IOException}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, count, monotonically_increasing_id, sum}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession, functions}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


class ReconAutomation {

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
  def readDataAndConvertToDataframe(readType: String, fileType: String, filePath: String, connString: String, driverName: String,
               database: String, table: String, user: String, password: String): DataFrame = {

    if (readType == "file") {
      try {
        spark.read.option("header", "true").option("inferSchema", "true").format(fileType).load(filePath)
      }

    }
    else if (readType == "database") {
      try {
        spark.read.format("jdbc").option("url", connString).option("driver", driverName)
          .option("dbtable", table).option("user", user).option("password", password).load()
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
      // Make sure that column name and column count are same
     if (!sourceDF.columns.sameElements(targetDF.columns))
      {
        println("Column names and count were different in source and target!")
        throw new Exception("Column count or column name didn't match!")
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
  def joinDF(sourceDF: DataFrame, targetDF: DataFrame, schemaSchemaList: List[String],
             joinType: String, alias: String): DataFrame = {

     sourceDF.na.fill(0).join(targetDF.na.fill(0), schemaSchemaList, joinType)
       .agg(count("*").as(alias))
       .withColumn("Column_Name", monotonically_increasing_id())
  }

  /**
   * Will calculate the number of records in source and target
   * @param df sourceDataFrame which have to compare with targetDataFrame
   * @param column of the source/target to be compare
   * @return  DataFrame
   */
  def rowsCount(df: DataFrame, column: List[String]): DataFrame = {

    val colRegex = raw"^.+\((.*?)\)".r
    val mapDF = column.map(_ -> "count").toMap
    // val mapDF = df.columns.map(_ -> "count").toMap
    val resDF = df.groupBy().agg(mapDF)
    val newCols = resDF.columns.map(x => col(x).as(colRegex.replaceAllIn(x, m => m.group(1))))
    val resultDF = resDF.select(newCols: _*)
      .na.fill(0)
      .withColumn("Column_Name", monotonically_increasing_id())
    resultDF
  }

  /**
   * Will join source and target dataframe inorder to get the extra records in source and target
   * @param joinType type of the join(left_anti)
   * @param columns of the the source/target dataframe excluding the primary key
   * @param sourceDF sourceDF
   * @param targetDF targetDF
   * @param primaryKey PrimaryKey of the source & Target it could be more than 1
   * @return  DataFrame
   */
  def joinDF(joinType: String, columns: List[String], sourceDF: DataFrame, targetDF: DataFrame
             , primaryKey: List[String]): DataFrame = {

    columns.map( i => sourceDF.join(targetDF, primaryKey:+i, joinType).agg(sum(i).as(i))
      .na.fill(0)
      .withColumn("Column_Name", monotonically_increasing_id()))
      .reduce((x, y) => x.join(y,"Column_Name"))

  }

}

object ReconAutomationObject {

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

    val sourceDF = new ReconAutomation().readDataAndConvertToDataframe(readType, fileType, sourcePath, connString,
      driverName, database, table, user, password)
    println("Source Data:")
    sourceDF.show()

    val targetDF = new ReconAutomation().readDataAndConvertToDataframe(readType, fileType, targetPath, connString,
      driverName, database, table, user, password)
    println("Target Data:")
    targetDF.show()

    // Schema of Source Data in List
    val schemaSchemaList = sourceDF.columns.toList

    // Columns to select after ignoring Primary Key
    val columnToSelect = schemaSchemaList diff primaryKeyList

    val sourceRecCount = new ReconAutomation().totalRecordCount(sourceDF,targetDF, "Source_Rec_Count")
    val targetRecCount = new ReconAutomation().totalRecordCount(targetDF,sourceDF, "Target_Rec_Count")

    // Overlap Record
    val overlapRecCount = new ReconAutomation()
      .joinDF(sourceDF, targetDF, schemaSchemaList, "inner", "Overlap_Rec_Count")

    // Extra Records in Source
    val extraSourceRecCount = new ReconAutomation()
      .joinDF(sourceDF, targetDF, schemaSchemaList, "left_anti", "Source_Extra_Rec_Count")

    // Extra Records in Target
    val extraTargetRecCount = new ReconAutomation()
      .joinDF(targetDF, sourceDF, schemaSchemaList, "left_anti","Target_Extra_Rec_Count" )

    val joinResult = sourceRecCount
      .join(targetRecCount, Seq("Column_Name"),"inner")
      .join(overlapRecCount, Seq("Column_Name"),"inner")
      .join(extraSourceRecCount, Seq("Column_Name"),"inner")
      .join(extraTargetRecCount, Seq("Column_Name"),"inner").drop("Column_Name")
    joinResult.show()

    val sourceRowCount = new ReconAutomation().rowsCount(sourceDF, columnToSelect)
    sourceRowCount.show()

    val targetRowCount = new ReconAutomation().rowsCount(targetDF, columnToSelect)
    targetRowCount.show()

  }
}