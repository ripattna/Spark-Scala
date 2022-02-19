package com.automationn

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, count, monotonically_increasing_id}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


class ReconAutomation1 {

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
  def readDataAndConvertToDataframe(readType: String,fileType: String,filePath: String,
                                    database: String,table: String,user: String,password: String,driverName: String,
                                    connString: String): DataFrame = {

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
      spark.read.option("header", "true").option("inferSchema", "true").format(fileType).load(filePath)
    }
  }

  /**
   * Will return the dataframe record count
   * @param sourceDF record count
   * @param targetDF record count
   * @return  DataFrame
   */
  def calculateTotalRecordCount(sourceDF: DataFrame, targetDF: DataFrame, alias: String): DataFrame = {

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
  def calculateJoinResult(sourceDF: DataFrame, targetDF: DataFrame, schemaSchemaList: List[String],
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
  def calculateRowsWiseRecordCount(df: DataFrame, column: List[String]): DataFrame = {

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
  def calculateJoinResultToGetRowWiseCount(joinType: String, columns: List[String], sourceDF: DataFrame, targetDF: DataFrame,
                                           primaryKey: List[String]): DataFrame = {

    columns.map( i => sourceDF.join(targetDF, primaryKey:+i, joinType).agg(count(i).as(i))
      .na.fill(0)
      .withColumn("Column_Name", monotonically_increasing_id()))
      .reduce((x, y) => x.join(y,"Column_Name"))
  }

  /**
   * Will method will transpose the dataframe
   * @param df
   * @param columns of the the source/target dataframe excluding the primary key
   * @param pivotCol sourceDF
   * @return  DataFrame
   */
  def transposeDataFrame(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {

    val columnsValue = columns.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")
    val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")")
      .select(pivotCol, "col0", "col1")
    val transposeDF = df_1.groupBy(col("col0")).pivot(pivotCol)
      .agg(concat_ws("", collect_list(col("col1"))))
      .withColumnRenamed("col0", pivotCol)
    transposeDF
  }
}

object ReconAutomationObject1 {

  def main(args: Array[String]): Unit = {

    // Reading the conf file
    val config: Config = ConfigFactory.load("configBck.conf")

    // Reading the file format from config
    val readType: String = config.getString("readType")

    // Reading the source and target file from config
    val sourcePath: String = config.getString("fileDetails.sourceFile")
    val targetPath: String = config.getString("fileDetails.targetFile")

    val fileType: String = config.getString("fileDetails.fileType")

    /**
     * Reading the source database connection
     **/
    val sourceDatabase : String = config.getString("databaseDetails.sourceDatabaseName")
    val sourceTable : String = config.getString("databaseDetails.sourceTableName")
    val sourceUser = config.getString("databaseDetails.sourceUserName")
    val sourcePassword =  config.getString("databaseDetails.sourcePassword")
    val sourceDriverName = config.getString("databaseDetails.sourceDriverName")
    val sourceConnString = config.getString("databaseDetails.sourceConnString")

    /**
     * Reading the target database connection
     **/
    val targetDatabase : String = config.getString("databaseDetails.targetDatabaseName")
    val targetTable : String = config.getString("databaseDetails.targetTableName")
    val targetUser = config.getString("databaseDetails.targetUserName")
    val targetPassword =  config.getString("databaseDetails.targetPassword")
    val targetDriverName = config.getString("databaseDetails.targetDriverName")
    val targetConnString = config.getString("databaseDetails.targetConnString")

    // Reading the PrimaryKey from config
    val primaryKeyList = config.getStringList("primaryKey.primaryKeyValue").toList

    /**
     * Now calling the method which is available in mysqlConnection class
     **/
    val sourceDF = new ReconAutomation1().readDataAndConvertToDataframe(readType,fileType,sourcePath,
      sourceDatabase, sourceTable, sourceUser, sourcePassword, sourceDriverName, sourceConnString)
    println("Source Data:")
    sourceDF.show()

    val targetDF = new ReconAutomation1().readDataAndConvertToDataframe(readType,fileType,targetPath,
      targetDatabase, targetTable, targetUser, targetPassword, targetDriverName, targetConnString)
    println("Target Data:")
    targetDF.show()

    // Schema of Source Data in List
    val schemaSchemaList = sourceDF.columns.toList

    // Columns to select after ignoring Primary Key
    val columnToSelect = schemaSchemaList diff primaryKeyList

    val sourceRecCount = new ReconAutomation1().calculateTotalRecordCount(sourceDF,targetDF, "Source_Rec_Count")
    val targetRecCount = new ReconAutomation1().calculateTotalRecordCount(targetDF,sourceDF, "Target_Rec_Count")

    // Overlap Record
    val overlapRecCount = new ReconAutomation1()
      .calculateJoinResult(sourceDF, targetDF, schemaSchemaList, "inner", "Overlap_Rec_Count")

    // Extra Records in Source
    val extraSourceRecCount = new ReconAutomation1()
      .calculateJoinResult(sourceDF, targetDF, schemaSchemaList, "left_anti", "Source_Extra_Rec_Count")

    // Extra Records in Target
    val extraTargetRecCount = new ReconAutomation1()
      .calculateJoinResult(targetDF, sourceDF, schemaSchemaList, "left_anti","Target_Extra_Rec_Count" )

    println("Output:1")
    val joinResult = sourceRecCount
      .join(targetRecCount, Seq("Column_Name"),"inner")
      .join(overlapRecCount, Seq("Column_Name"),"inner")
      .join(extraSourceRecCount, Seq("Column_Name"),"inner")
      .join(extraTargetRecCount, Seq("Column_Name"),"inner").drop("Column_Name")
    joinResult.show()

    val sourceRowCount = new ReconAutomation1().calculateRowsWiseRecordCount(sourceDF, columnToSelect)
    // sourceRowCount.show()

    val targetRowCount = new ReconAutomation1().calculateRowsWiseRecordCount(targetDF, columnToSelect)
    // targetRowCount.show()

    // Overlap Records
    val overlapRowCount = new ReconAutomation1()
      .calculateJoinResultToGetRowWiseCount("inner", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // overlapRowCount.show()

    // Extra Records in Source
    val extraSourceRowCount = new ReconAutomation1()
      .calculateJoinResultToGetRowWiseCount("left_anti", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // extraSourceRowCount.show()

    // Extra Records in Target
    val extraTargetRowCount = new ReconAutomation1()
      .calculateJoinResultToGetRowWiseCount("left_anti",  columnToSelect, targetDF, sourceDF, primaryKeyList)
    // extraTargetRowCount.show()

    // Transpose the result
    val sourceRowsCount = new ReconAutomation1()
      .transposeDataFrame(sourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Source_Rec_Count")

    val targetRowsCount = new ReconAutomation1()
      .transposeDataFrame(targetRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Target_Rec_Count")

    val overlapRowsCount = new ReconAutomation1()
      .transposeDataFrame(overlapRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Overlap_Rec_Count")

    val extraSourceRowsCount = new ReconAutomation1()
      .transposeDataFrame(extraSourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Source_Extra_Rec_Count")

    val extraTargetRowsCount = new ReconAutomation1()
      .transposeDataFrame(extraTargetRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Target_Extra_Rec_Count")

    // Final Result DF
    println("Output:2")
    val finalDF = sourceRowsCount
      .join(targetRowsCount, Seq("Column_Name"),"inner")
      .join(overlapRowsCount, Seq("Column_Name"),"inner")
      .join(extraSourceRowsCount, Seq("Column_Name"),"inner")
      .join(extraTargetRowsCount, Seq("Column_Name"),"inner")
    finalDF.show()

    // Write DataFrame data to CSV file
    finalDF.write
      .format("csv")
      .option("header", true)
      .mode("overwrite")
      .save("/tmp/reconOutput")
  }
}