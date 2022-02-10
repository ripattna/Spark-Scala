package com.automationn

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, count, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
   * @param df record count
   * @return  DataFrame
   */
  def totalRecordCount(df: DataFrame, alias: String): DataFrame = {

    df.agg(count("*").as(alias))
      .withColumn("Column_Name", monotonically_increasing_id())

  }

  /**
   * Will join source and target dataframe inorder to get the extra records in source and target
   * @param joinType type of the join(left_anti)
   * @param sourceDF sourceDF
   * @param targetDF targetDF
   * @param schemaSchemaList
   * @return  DataFrame
   */
  def joinDF(sourceDF: DataFrame,targetDF: DataFrame,
             schemaSchemaList: List[String], joinType: String, alias: String): DataFrame = {

     sourceDF.join(targetDF, schemaSchemaList, joinType)
       .agg(count("*").as(alias))
       .withColumn("Column_Name", monotonically_increasing_id())
  }

  /**
   * Will method will transpose the dataframe
   * @param df
   * @param columns of the the source/target dataframe excluding the primary key
   * @param pivotCol sourceDF
   * @return  DataFrame
   */
  def TransposeDF(df: DataFrame, schemaSchemaList: Seq[String], pivotCol: String): DataFrame = {
    val columnsValue = schemaSchemaList.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")
    val df_1 = df.selectExpr(pivotCol, "stack(" + schemaSchemaList.size + "," + stackCols + ")")
      .select(pivotCol, "col0", "col1")
    val transposeDF = df_1.groupBy(col("col0")).pivot(pivotCol)
      .agg(concat_ws("", collect_list(col("col1"))))
      .withColumnRenamed("col0", pivotCol)
    transposeDF
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

    val sourceRowCount = new mysqlConnection().totalRecordCount(sourceDF, "Source_Rec_Count")
    sourceRowCount.show()
    val targetRowCount = new mysqlConnection().totalRecordCount(targetDF, "Target_Rec_Count")
    targetRowCount.show()

    val overlapRowCount = new mysqlConnection().joinDF(sourceDF, targetDF, schemaSchemaList, "inner", "Overlap_Rec_Count")
    overlapRowCount.show()

    // Extra Records in Source
    val extraSourceRowCount = new mysqlConnection()
      .joinDF(sourceDF, targetDF, schemaSchemaList, "left_anti", "Source_Extra_Rec_Count")
    extraSourceRowCount.show()

    // Extra Records in Target
    val extraTargetRowCount = new mysqlConnection()
      .joinDF(targetDF, sourceDF, schemaSchemaList, "left_anti","Target_Extra_Rec_Count" )
    extraTargetRowCount.show()

    // Transpose the result
    val sourceRowsCount = new mysqlConnection()
      .TransposeDF(sourceRowCount, schemaSchemaList, "Column_Name")
      .withColumnRenamed("0","Source_Rec_Count")
    sourceRowCount.show()

    val targetRowsCount = new mysqlConnection()
      .TransposeDF(targetRowCount, schemaSchemaList, "Column_Name")
      .withColumnRenamed("0","Target_Rec_Count")

    val overlapRowsCount = new mysqlConnection()
      .TransposeDF(overlapRowCount, schemaSchemaList, "Column_Name")
      .withColumnRenamed("0","Overlap_Rec_Count")

    val extraSourceRowsCount = new mysqlConnection()
      .TransposeDF(extraSourceRowCount, schemaSchemaList, "Column_Name")
      .withColumnRenamed("0","Source_Extra_Rec_Count")

    val extraTargetRowsCount = new mysqlConnection()
      .TransposeDF(extraTargetRowCount, schemaSchemaList, "Column_Name")
      .withColumnRenamed("0","Target_Extra_Rec_Count")

    // Final Result DF
    val finalDF = sourceRowsCount
      .join(targetRowsCount, Seq("Column_Name"),"inner")
      .join(overlapRowsCount, Seq("Column_Name"),"inner")
      .join(extraSourceRowsCount, Seq("Column_Name"),"inner")
      .join(extraTargetRowsCount, Seq("Column_Name"),"inner")
    finalDF.show()


  }
}