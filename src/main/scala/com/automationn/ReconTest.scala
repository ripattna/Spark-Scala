package com.automationn

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class ReconTest{

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
   * @param fileFormat could be parquet,csv,json  etc
   * @param filePath where the file reside in any of the storage
   * @return  DataFrame
   */
  // Read the source and target file
  def readFile(fileType: String, fileFormat: String, filePath: String): DataFrame = {

    if (fileType == "file") {
      spark.read.option("header", "true")
        .option("inferSchema", "true")
        .format(fileFormat)
        .load(filePath)
    }

    else if (fileType == "database") {
      val database = "demo"
      val table = "employee"
      val user = "root"
      val password = "root"
      val driverName = "com.mysql.cj.jdbc.Driver"
      val connString = "jdbc:mysql://localhost:3306/" + database

      spark.read.format("jdbc").option("url", connString).option("driver", driverName)
        .option("dbtable", table).option("user", user).option("password", password).load()
    }

    else {
      spark.read.format("jdbc").load()
    }
  }

  /**
   * Will calculate the number of records in source and target
   * @param df sourceDataFrame which have to compare with targetDataFrame
   * @param column of the source/target to be compare
   * @return  DataFrame
   */
  def rowsCount(df: DataFrame, column: List[String]): DataFrame = {
    val newDF = df.groupBy().sum(column: _*)
    val colRegex = raw"^.+\((.*?)\)".r
    val newCols = newDF.columns.map(x => col(x).as(colRegex.replaceAllIn(x, m => m.group(1))))
    val resultDF = newDF.select(newCols: _*)
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
    val resultSet = columns
      .map( (i => sourceDF.join(targetDF, primaryKey:+i, joinType).agg(sum(i).as(i))
        .na.fill(0)
        .withColumn("Column_Name", monotonically_increasing_id())))
      .reduce((x, y) => x.join(y,"Column_Name"))
    resultSet
  }

  /**
   * Will method will transpose the dataframe
   * @param df
   * @param columns of the the source/target dataframe excluding the primary key
   * @param pivotCol sourceDF
   * @return  DataFrame
   */
  def TransposeDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
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

object ReconTestObject {

  def main(args:Array[String]){

    // Reading the conf file
    val applicationConf: Config = ConfigFactory.load("application.conf")

    // Reading the source and target file from config
    val sourcePath: String = applicationConf.getString("filePath.sourceFile")
    val targetPath: String = applicationConf.getString("filePath.targetFile")

    // Reading the file format from config
    val fileFormat: String = applicationConf.getString("fileDetails.fileFormat")
    val fileType: String = applicationConf.getString("fileDetails.fileType")

    // Reading the PrimaryKey from config
    val primaryKeyList = applicationConf.getStringList("primaryKey.primaryKeyValue").toList

    val sourceDF = new ReconTest().readFile(fileType,fileFormat, sourcePath)
    println("Source Data:")
    sourceDF.show()
    val targetDF = new ReconTest().readFile(fileType,fileFormat, targetPath)
    println("Target Data:")
    targetDF.show()

    // Schema of Source Data in List
    val schemaSchemaList = sourceDF.columns.toList
    // println(schemaSchemaList)

    // Columns to select after ignoring Primary Key
    val columnToSelect = schemaSchemaList diff primaryKeyList
    // println(columnToSelect)

    val sourceRowCount = new ReconTest().rowsCount(sourceDF, columnToSelect)
    sourceRowCount.show()

    val targetRowCount = new ReconTest().rowsCount(targetDF, columnToSelect)
    targetRowCount.show()

    // Overlap Records
    val overlapRowCount = new ReconTest()
      .joinDF("inner", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // overlapRowCount.show()

    // Extra Records in Source
    val extraSourceRowCount = new ReconTest()
      .joinDF("left_anti", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // extraSourceRowCount.show()

    // Extra Records in Target
    val extraTargetRowCount = new ReconTest()
      .joinDF("left_anti",  columnToSelect, targetDF, sourceDF, primaryKeyList)
    // extraTargetRowCount.show()

    // Transpose the result
    val sourceRowsCount = new ReconTest()
      .TransposeDF(sourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","No_Of_Rec_Source")

    val targetRowsCount = new ReconTest()
      .TransposeDF(targetRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","No_Of_Rec_Target")

    val overlapRowsCount = new ReconTest()
      .TransposeDF(overlapRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Overlap_Count")

    val extraSourceRowsCount = new ReconTest()
      .TransposeDF(extraSourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Extra_Rec_Source")

    val extraTargetRowsCount = new ReconTest()
      .TransposeDF(extraTargetRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Extra_Rec_Target")

    // Final Result DF
    val finalDF = sourceRowsCount
      .join(targetRowsCount, Seq("Column_Name"),"inner")
      .join(overlapRowsCount, Seq("Column_Name"),"inner")
      .join(extraSourceRowsCount, Seq("Column_Name"),"inner")
      .join(extraTargetRowsCount, Seq("Column_Name"),"inner")
    finalDF.show()

    // Write DataFrame data to CSV file
    // finalDF.write.format("csv").option("header", true).mode("overwrite").save("/tmp/reconRes")

  }
}