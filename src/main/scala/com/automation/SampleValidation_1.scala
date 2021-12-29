package com.automation

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object SampleValidation_1 {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("ERROR")

    // Read the source file
    val sourceDF = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Project\\Files\\df1.csv")
    println("Printing the Source DF:")
    sourceDF.show()

    // Read the target file
    val targetDF = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Project\\Files\\df2.csv")
    println("Printing the Target DF:")
    targetDF.show()

    // For No of Rows in Source where value is 1
    println("For No of Rows in Source where value is 1:")
    val sourceAgg = sourceDF.agg(sum("valueA"), sum("valueB"), sum("valueC"), sum("valueD"))
      .select(col("sum(valueA)").as("valueA"), col("sum(valueB)").as("valueB"), col("sum(valueC)").as("valueC"), col("sum(valueD)").as("valueD"))
      .withColumn("Column_Name",monotonically_increasing_id())

    def TransposeSourceDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
      val columnsValue = columns.map(x => "'" + x + "', " + x)
      val stackCols = columnsValue.mkString(",")
      val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")").select(pivotCol, "col0", "col1")
      val final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1")))).withColumnRenamed("col0", pivotCol)
      final_df
    }
    val sourceRowsCount = TransposeSourceDF(sourceAgg, Seq("valueA", "valueB", "valueC", "valueD"), "Column_Name").withColumnRenamed("0","No_Of_Rows_Source")
    sourceRowsCount.show()

    // For No of Rows in Target where value is 1
    println("For No of Rows in Target where value is 1:")
    val targetAgg = targetDF.agg(sum("valueA"), sum("valueB"), sum("valueC"), sum("valueD"))
      .select(col("sum(valueA)").as("valueA"), col("sum(valueB)").as("valueB"), col("sum(valueC)").as("valueC"), col("sum(valueD)").as("valueD"))
      .withColumn("Column_Name",monotonically_increasing_id())

    def TransposeTargetDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
      val columnsValue = columns.map(x => "'" + x + "', " + x)
      val stackCols = columnsValue.mkString(",")
      val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")").select(pivotCol, "col0", "col1")
      val final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1")))).withColumnRenamed("col0", pivotCol)
      final_df
    }
    val targetRowsCount = TransposeTargetDF(targetAgg, Seq("valueA", "valueB", "valueC", "valueD"), "Column_Name").withColumnRenamed("0","No_Of_Rows_Target")
    targetRowsCount.show()

    // Source and Target Row Count
    val sourceAndTargetCount = sourceRowsCount.join(targetRowsCount, Seq("Column_Name"),"inner")
    sourceAndTargetCount.show()

    // Inner join to get the Overlap Count
    val valueAInner = sourceDF.join(targetDF, Seq("Primary_Key","valueA"),"inner").agg(sum("valueA")).select(col("sum(valueA)").as("valueA")).withColumn("index",monotonically_increasing_id())
    val valueBInner = sourceDF.join(targetDF, Seq("Primary_Key","valueB"),"inner").agg(sum("valueB")).select(col("sum(valueB)").as("valueB")).withColumn("index",monotonically_increasing_id())
    val valueCInner = sourceDF.join(targetDF, Seq("Primary_Key","valueC"),"inner").agg(sum("valueC")).select(col("sum(valueC)").as("valueC")).withColumn("index",monotonically_increasing_id())
    val valueDInner = sourceDF.join(targetDF, Seq("Primary_Key","valueD"),"inner").agg(sum("valueD")).select(col("sum(valueD)").as("valueD")).withColumn("index",monotonically_increasing_id())

    val innerJoinAB = valueAInner.join(valueBInner, Seq("index"))
    val innerJoinCD = valueCInner.join(valueDInner, Seq("index"))
    val innerJoin = innerJoinAB.join(innerJoinCD,Seq("index")).na.fill(0).withColumn("Column_Name",monotonically_increasing_id()).drop("index")
    // println("Inner join result:")
    // innerJoin.show()

    def TransposeInnerDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
      val columnsValue = columns.map(x => "'" + x + "', " + x)
      val stackCols = columnsValue.mkString(",")
      val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")").select(pivotCol, "col0", "col1")
      val final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1")))).withColumnRenamed("col0", pivotCol)
      final_df
    }
    val innerCount = TransposeInnerDF(innerJoin, Seq("valueA", "valueB", "valueC", "valueD"), "Column_Name").withColumnRenamed("0","Overlap_Count")
    innerCount.show()

    // Including the inner join result in summery
    val innerJoinResult = sourceAndTargetCount.join(innerCount,Seq("Column_Name"))
    innerJoinResult.show()

    val valueALeftJoin = sourceDF.join(targetDF, Seq("Primary_Key","valueA"),"left_anti").agg(sum("valueA")).select(col("sum(valueA)").as("valueA")).withColumn("index",monotonically_increasing_id())
    val valueBLeftJoin = sourceDF.join(targetDF, Seq("Primary_Key","valueB"),"left_anti").agg(sum("valueB")).select(col("sum(valueB)").as("valueB")).withColumn("index",monotonically_increasing_id())
    val valueCInnerJoin = sourceDF.join(targetDF, Seq("Primary_Key","valueC"),"left_anti").agg(sum("valueC")).select(col("sum(valueC)").as("valueC")).withColumn("index",monotonically_increasing_id())
    val valueDInnerJoin = sourceDF.join(targetDF, Seq("Primary_Key","valueD"),"left_anti").agg(sum("valueD")).select(col("sum(valueD)").as("valueD")).withColumn("index",monotonically_increasing_id())

    val leftJoinAB = valueALeftJoin.join(valueBLeftJoin, Seq("index"))
    val leftJoinCD = valueCInnerJoin.join(valueDInnerJoin, Seq("index"))
    val leftJoin = leftJoinAB.join(leftJoinCD,Seq("index")).na.fill(0).withColumn("Column_Name",monotonically_increasing_id()).drop("index")
    println("Present in Source not in Target:")
    leftJoin.show()

    def TransposeLeftAntiDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
      val columnsValue = columns.map(x => "'" + x + "', " + x)
      val stackCols = columnsValue.mkString(",")
      val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")").select(pivotCol, "col0", "col1")
      val final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1")))).withColumnRenamed("col0", pivotCol)
      final_df
    }

    val leftJoinCount = TransposeLeftAntiDF(leftJoin, Seq("valueA", "valueB", "valueC", "valueD"),"Column_Name").withColumnRenamed("0","Extra_Record_Source_V_1")
    leftJoinCount.show()

    // Including the left_anti join result in summery
    val leftAntiJoinResult = innerJoinResult.join(leftJoinCount,Seq("Column_Name")).orderBy("Column_Name")
    leftAntiJoinResult.show()

    // Right Anti Join
    val valueARightJoin = targetDF.join(sourceDF, Seq("Primary_Key","valueA"),"left_anti").agg(sum("valueA")).select(col("sum(valueA)").as("valueA")).withColumn("index",monotonically_increasing_id())
    val valueBRightJoin = targetDF.join(sourceDF, Seq("Primary_Key","valueB"),"left_anti").agg(sum("valueB")).select(col("sum(valueB)").as("valueB")).withColumn("index",monotonically_increasing_id())
    val valueCRightJoin = targetDF.join(sourceDF, Seq("Primary_Key","valueC"),"left_anti").agg(sum("valueC")).select(col("sum(valueC)").as("valueC")).withColumn("index",monotonically_increasing_id())
    val valueDRightJoin = targetDF.join(sourceDF, Seq("Primary_Key","valueD"),"left_anti").agg(sum("valueD")).select(col("sum(valueD)").as("valueD")).withColumn("index",monotonically_increasing_id())

    val rightJoinAB = valueARightJoin.join(valueBRightJoin, Seq("index"))
    val rightJoinCD = valueCRightJoin.join(valueDRightJoin, Seq("index"))
    val rightJoin = rightJoinAB.join(rightJoinCD,Seq("index")).na.fill(0).withColumn("Column_Name",monotonically_increasing_id()).drop("index")
    println("Present in Source not in Target:")
    rightJoin.show()

    def TransposeRightAntiDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
      val columnsValue = columns.map(x => "'" + x + "', " + x)
      val stackCols = columnsValue.mkString(",")
      val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")").select(pivotCol, "col0", "col1")
      val final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1")))).withColumnRenamed("col0", pivotCol)
      final_df
    }

    val rightJoinCount = TransposeRightAntiDF(rightJoin, Seq("valueA", "valueB", "valueC", "valueD"),"Column_Name").withColumnRenamed("0","Extra_Record_Target_V_1")
    rightJoinCount.show()

    println("Final Result:")
    val final_df = leftAntiJoinResult.join(rightJoinCount,Seq("Column_Name")).orderBy("Column_Name").na.fill(0)
    final_df.show()

  }

}
