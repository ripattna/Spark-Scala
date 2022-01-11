package com.automation

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.sql.functions.col
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


object SampleValidation {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Reading the conf file
    val applicationConf: Config = ConfigFactory.load("application.conf")

    val sourcePath: String = applicationConf.getString("filePath.sourceFile")
    val targetPath: String = applicationConf.getString("filePath.targetFile")

    // File Format
    val fileType: String = applicationConf.getString("fileFormat.fileType")
    // println(fileType)

    val primaryKeyList = applicationConf.getStringList("primaryKey.primaryKeyValue").toList
    // println(primaryKeyList)

    val primaryKeyListString = primaryKeyList.mkString(",")
    // println(primaryKeyListString)

    // Read the source and target file
    def readFile(path: String): DataFrame = {
      val df = spark.read.option("header", "true").option("inferSchema", "true").format(fileType).load(path)
      df
    }

    // Source DF
    val sourceDF = readFile(sourcePath)
    // Target DF
    val targetDF = readFile(targetPath)

    // Schema of Source Data in List
    val schemaSchemaList = sourceDF.columns.toList
    // println(schemaSchemaList)

    // Columns to select after ignoring Primary Key
    val columnToSelect = schemaSchemaList diff primaryKeyList
    // println(columnToSelect)

    def rowsCount(df: DataFrame): DataFrame = {
      val newDF = df.groupBy().sum(columnToSelect: _*)
      val colRegex = raw"^.+\((.*?)\)".r
      val newCols = newDF.columns.map(x => col(x).as(colRegex.replaceAllIn(x, m => m.group(1))))
      val resultDF = newDF.select(newCols: _*)
        .na.fill(0)
        .withColumn("Column_Name", monotonically_increasing_id())
      resultDF
    }

    val sourceRowCount = rowsCount(sourceDF)
    // sourceRowCount.show()
    val targetRowCount = rowsCount(targetDF)
    // targetRowCount.show()

    def joinResult(joinType: String): DataFrame ={
      var resultSet = columnToSelect.map( i => (sourceDF.join(targetDF, Seq(primaryKeyListString, i), joinType).agg(sum(i).as(i))
        .na.fill(0)
        .withColumn("Column_Name", monotonically_increasing_id())))
        .reduce((x, y) => x.join(y,"Column_Name"))
      resultSet
    }

    // Overlap Records
    val overlapRowCount = joinResult("inner")
    // overlapRowCount.show()

    // Extra Records in Source
    val extraSourceRowCount = joinResult("left_anti")
    // extraSourceRowCount.show()

    def extraRecordTarget(joinType: String): DataFrame ={
      var extraTargetResultSet = columnToSelect.map( i => (targetDF.join(sourceDF, Seq(primaryKeyListString, i), joinType).agg(sum(i).as(i))
        .na.fill(0)
        .withColumn("Column_Name", monotonically_increasing_id())))
        .reduce((x, y) => x.join(y,"Column_Name"))
      extraTargetResultSet
    }
    // Extra Records in Target
    val extraTargetRowCount = extraRecordTarget("left_anti")
    // extraTargetRowCount.show()

    def TransposeDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
      val columnsValue = columns.map(x => "'" + x + "', " + x)
      val stackCols = columnsValue.mkString(",")
      val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")").select(pivotCol, "col0", "col1")
      val transposeDF = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1")))).withColumnRenamed("col0", pivotCol)
      transposeDF
    }

    val sourceRowsCount = TransposeDF(sourceRowCount, columnToSelect, "Column_Name").withColumnRenamed("0","No_Of_Rec_Source")
    val targetRowsCount = TransposeDF(targetRowCount, columnToSelect, "Column_Name").withColumnRenamed("0","No_Of_Rec_Target")
    val overlapRowsCount = TransposeDF(overlapRowCount, columnToSelect, "Column_Name").withColumnRenamed("0","Overlap_Count")
    val extraSourceRowsCount = TransposeDF(extraSourceRowCount, columnToSelect, "Column_Name").withColumnRenamed("0","Extra_Rec_Source")
    val extraTargetRowsCount = TransposeDF(extraTargetRowCount, columnToSelect, "Column_Name").withColumnRenamed("0","Extra_Rec_Target")

    // Final Result DF
    val finalDF = sourceRowsCount
      .join(targetRowsCount, Seq("Column_Name"),"inner")
      .join(overlapRowsCount, Seq("Column_Name"),"inner")
      .join(extraSourceRowsCount, Seq("Column_Name"),"inner")
      .join(extraTargetRowsCount, Seq("Column_Name"),"inner")
    finalDF.show()

  }

}
