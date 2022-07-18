package com.automation

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.sql.functions.col
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


object SampleValidation_1 {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Reading the conf file
    val applicationConf: Config = ConfigFactory.load("Config/application.conf")

    val sourcePath  = applicationConf.getString("filePath.sourceFile")
    val targetPath  = applicationConf.getString("filePath.targetFile")

    val primaryKeyList = applicationConf.getStringList("primaryKey.primaryKeyValue").toList
    println(primaryKeyList)

    val primaryKeyListString = primaryKeyList.mkString(",")
    println(primaryKeyListString)

    // Read the source and target file
    def readFile(path: String): DataFrame = {
      val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
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
    sourceRowCount.show()
    val targetRowCount = rowsCount(targetDF)
    targetRowCount.show()

    val col1 = columnToSelect(0)
    val col2 = columnToSelect(1)
    val col3 = columnToSelect(2)
    val col4 = columnToSelect(3)

    // Inner join to get the Overlap Count

    def joinResult(joinType: String): DataFrame ={

      val valueAInner = sourceDF.join(targetDF, Seq(primaryKeyListString, col1), joinType).agg(sum(col1).as(col1)).withColumn("index", monotonically_increasing_id())
      val valueBInner = sourceDF.join(targetDF, Seq(primaryKeyListString, col2), joinType).agg(sum(col2).as(col2)).withColumn("index", monotonically_increasing_id())
      val valueCInner = sourceDF.join(targetDF, Seq(primaryKeyListString, col3), joinType).agg(sum(col3).as(col3)).withColumn("index", monotonically_increasing_id())
      val valueDInner = sourceDF.join(targetDF, Seq(primaryKeyListString, col4), joinType).agg(sum(col4).as(col4)).withColumn("index", monotonically_increasing_id())

      val innerJoinAB = valueAInner.join(valueBInner, Seq("index"))
      val innerJoinCD = valueCInner.join(valueDInner, Seq("index"))
      val resultSet = innerJoinAB.join(innerJoinCD,Seq("index")).na.fill(0)
        .withColumn("Column_Name", monotonically_increasing_id()).drop("index")

      resultSet
    }

    // Overlap records
    val overlapRowCount = joinResult("inner")
    overlapRowCount.show()

    // Extra records in source
    val extraSourceRowCount = joinResult("left_anti")
    extraSourceRowCount.show()

    def extraRecordTarget(joinType: String): DataFrame ={

      val valueAInner = targetDF.join(sourceDF, Seq(primaryKeyListString, col1), joinType).agg(sum(col1).as(col1)).withColumn("index", monotonically_increasing_id())
      val valueBInner = targetDF.join(sourceDF, Seq(primaryKeyListString, col2), joinType).agg(sum(col2).as(col2)).withColumn("index", monotonically_increasing_id())
      val valueCInner = targetDF.join(sourceDF, Seq(primaryKeyListString, col3), joinType).agg(sum(col3).as(col3)).withColumn("index", monotonically_increasing_id())
      val valueDInner = targetDF.join(sourceDF, Seq(primaryKeyListString, col4), joinType).agg(sum(col4).as(col4)).withColumn("index", monotonically_increasing_id())

      val innerJoinAB = valueAInner.join(valueBInner, Seq("index"))
      val innerJoinCD = valueCInner.join(valueDInner, Seq("index"))
      val extraTargetResultSet = innerJoinAB.join(innerJoinCD,Seq("index")).na.fill(0)
        .withColumn("Column_Name", monotonically_increasing_id()).drop("index")

      extraTargetResultSet

    }

    val extraTargetRowCount = extraRecordTarget("left_anti")
    extraTargetRowCount.show()

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
