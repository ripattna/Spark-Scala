package com.validation

import org.apache.spark.sql.functions.{col, collect_list, concat_ws, monotonically_increasing_id, sum}
import org.apache.spark.sql.{DataFrame, _}

class ReconValidation extends Step {

  // Read the source and target file
  def readFile(fileFormat: String, path: String): DataFrame = {
    val df = spark.read.option("header", "true").option("inferSchema", "true").format(fileFormat).load(path)
    df
  }

  def rowsCount(df: DataFrame, column: List[String]): DataFrame = {
    val newDF = df.groupBy().sum(column: _*)
    val colRegex = raw"^.+\((.*?)\)".r
    val newCols = newDF.columns.map(x => col(x).as(colRegex.replaceAllIn(x, m => m.group(1))))
    val resultDF = newDF
      .select(newCols: _*)
      .na
      .fill(0)
      .withColumn("Column_Name", monotonically_increasing_id())
    resultDF
  }

  def joinDF(joinType: String,
             columns: List[String],
             sourceDF: DataFrame,
             targetDF: DataFrame,
             primaryKey: List[String]): DataFrame = {
    val resultSet = columns
      .map(
        i =>
          (sourceDF
            .join(targetDF, primaryKey :+ i, joinType)
            .agg(sum(i).as(i))
            .na
            .fill(0)
            .withColumn("Column_Name", monotonically_increasing_id())))
      .reduce((x, y) => x.join(y, "Column_Name"))
    resultSet
  }

  def TransposeDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
    val columnsValue = columns.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")
    val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")").select(pivotCol, "col0", "col1")
    val transposeDF = df_1
      .groupBy(col("col0"))
      .pivot(pivotCol)
      .agg(concat_ws("", collect_list(col("col1"))))
      .withColumnRenamed("col0", pivotCol)
    transposeDF
  }

}
