package com.automation

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, monotonically_increasing_id, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class ReconAutomation{

  // Spark Session
  val spark = SparkSession.builder().master("local").appName("ReconAutomation").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // Read the source and target file
  def readFile(fileFormat: String, path: String): DataFrame = {
    val df = spark.read.option("header", "true").option("inferSchema", "true").format(fileFormat).load(path)
    df
  }

  def rowsCount(df: DataFrame, column: List[String]): DataFrame = {
    val newDF = df.groupBy().sum(column: _*)
    val colRegex = raw"^.+\((.*?)\)".r
    val newCols = newDF.columns.map(x => col(x).as(colRegex.replaceAllIn(x, m => m.group(1))))
    val resultDF = newDF.select(newCols: _*)
      .na.fill(0)
      .withColumn("Column_Name", monotonically_increasing_id())
    resultDF
  }

  def joinDF(joinType: String, columns: List[String],sourceDF: DataFrame,targetDF: DataFrame,primaryKey: List[String]): DataFrame ={
    val resultSet = columns.map( i => (sourceDF.join(targetDF, primaryKey:+i, joinType).agg(sum(i).as(i))
      .na.fill(0)
      .withColumn("Column_Name", monotonically_increasing_id())))
      .reduce((x, y) => x.join(y,"Column_Name"))
    resultSet
  }

  def TransposeDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
    val columnsValue = columns.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")
    val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")").select(pivotCol, "col0", "col1")
    val transposeDF = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1")))).withColumnRenamed("col0", pivotCol)
    transposeDF
  }

}

object ReconAutomationObject {

  def main(args:Array[String]){

    // Reading the conf file
    val applicationConf: Config = ConfigFactory.load("Config/application.conf")

    // Reading the source and target file from config
    val sourcePath: String = applicationConf.getString("filePath.sourceFile")
    val targetPath: String = applicationConf.getString("filePath.targetFile")

    // Reading the file format from config
    val fileType: String = applicationConf.getString("fileFormat.fileType")

    // Reading the PrimaryKey from config
    val primaryKeyList = applicationConf.getStringList("primaryKey.primaryKeyValue").toList

    val sourceDF = new ReconAutomation().readFile(fileType, sourcePath)
    println("Source Data:")
    sourceDF.show()
    val targetDF = new ReconAutomation().readFile(fileType, targetPath)
    println("Target Data:")
    targetDF.show()

    // Schema of Source Data in List
    val schemaSchemaList = sourceDF.columns.toList
    // println(schemaSchemaList)

    // Columns to select after ignoring Primary Key
    val columnToSelect = schemaSchemaList diff primaryKeyList
    // println(columnToSelect)

    val sourceRowCount = new ReconAutomation().rowsCount(sourceDF, columnToSelect)
    // sourceRowCount.show()
    val targetRowCount = new ReconAutomation().rowsCount(targetDF, columnToSelect)
    // targetRowCount.show()

    // Overlap Records
    val overlapRowCount = new ReconAutomation().joinDF("inner", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // overlapRowCount.show()

    // Extra Records in Source
    val extraSourceRowCount = new ReconAutomation().joinDF("left_anti", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // extraSourceRowCount.show()

    // Extra Records in Target
    val extraTargetRowCount = new ReconAutomation().joinDF("left_anti",  columnToSelect, targetDF, sourceDF, primaryKeyList)
    // extraTargetRowCount.show()

    // Transpose the result
    val sourceRowsCount = new ReconAutomation().TransposeDF(sourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","No_Of_Rec_Source")
    val targetRowsCount = new ReconAutomation().TransposeDF(targetRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","No_Of_Rec_Target")
    val overlapRowsCount = new ReconAutomation().TransposeDF(overlapRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Overlap_Count")
    val extraSourceRowsCount = new ReconAutomation().TransposeDF(extraSourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Extra_Rec_Source")
    val extraTargetRowsCount = new ReconAutomation().TransposeDF(extraTargetRowCount, columnToSelect, "Column_Name")
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