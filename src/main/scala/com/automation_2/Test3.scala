package com.automation_2

import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit, when}

object Test3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test3").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/Data/File_1.csv")
    df1.show()
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/Data/File_2.csv")
    df2.show()

    val columns = df1.columns
    val joinResult = df1.alias("d1").join(df2.alias("d2"), col("d1.ID") === col("d2.ID"), "left")

    val compRes = columns.foldLeft(joinResult) {(df, name) => df.withColumn(name + "_M", when(col("d1." + name) =!= col("d2." + name), lit(name)))}
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col(name + "_M")): _*))
    compRes.show()

    val compResult = columns.foldLeft(joinResult) {(df, name) => df.withColumn("M_" + name, when(col("d1." + name) === col("d2." + name), lit("Y")).otherwise(lit("N")))}
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col("M_" + name)): _*))
    compResult.show()

    /*

 val result = df1.join(df2, df1.col("ID") === df2.col("ID"))
   .withColumn("M_ID", when(df1.col("ID") === df2.col("ID"), lit("Y")).otherwise(lit("N")))
   .withColumn("M_Product", when(df1.col("Product") === df2.col("Product"), lit("Y")).otherwise(lit("N")))
   .withColumn("M_Country", when(df1.col("Country") === df2.col("Country"), lit("Y")).otherwise(lit("N")))
   .withColumn("M_Quantity", when(df1.col("Quantity") === df2.col("Quantity"), lit("Y")).otherwise(lit("N")))
   .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
 result.show(false)

 val primaryKeyList = Seq("ID")
 println(primaryKeyList)
 val schemaSchemaList = df1.columns.toList
 val columnToSelect = schemaSchemaList diff primaryKeyList

 val joinResult = df1.join(df2, primaryKeyList, "inner")
 joinResult.show()
 // val leftResult = df1.as("df1").join(df2.as("df2"), Seq("primaryKeyList"), "left")
 // val rightResult = df1.as("df1").join(df2.as("df2"), Seq("primaryKeyList"), "right")

 val innerRes = df1.as("df1").join(df2.as("df2"), Seq("ID","Product", "Country","Quantity"), "inner")
 val leftRes = df1.as("df1").join(df2.as("df2"), Seq("ID","Product", "Country","Quantity"), "left")
 val rightRes = df1.as("df1").join(df2.as("df2"), Seq("ID","Product", "Country","Quantity"), "right")
 innerRes.show()
 leftRes.show()
 rightRes.show()

  def compareSchemaDataFrames(left: DataFrame , leftViewName: String, right: DataFrame , rightViewName: String) :Pair[DataFrame, DataFrame] = {
   // Make sure that column names match in both dataFrames
   if (!left.columns.sameElements(right.columns))
   {
     println("column names were different")
     throw new Exception("Column Names Did Not Match")
   }

   val leftCols = left.columns.mkString(",")
   val rightCols = right.columns.mkString(",")

   //group by all columns in both data frames
   val groupedLeft = left.sqlContext.sql("select " + leftCols + " , count(*) as recordRepeatCount from " +  leftViewName + " group by " + leftCols )
   val groupedRight = left.sqlContext.sql("select " + rightCols + " , count(*) as recordRepeatCount from " +  rightViewName + " group by " + rightCols )

   //do the except/subtract command
   val inLnotinR = groupedLeft.except(groupedRight).toDF()
   val inRnotinL = groupedRight.except(groupedLeft).toDF()

   return new ImmutablePair[DataFrame, DataFrame](inLnotinR, inRnotinL)

 }

  */

  }
}
