package com.automation_2

import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit,  when}


object Test {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("ReconAutomation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/File_1.csv")
    df1.show()
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/File_2.csv")
    df2.show()

    val result = df1.join(df2, df1.col("ID") === df2.col("ID"))
      .withColumn("M_Product", when(df1.col("Product") === df2.col("Product"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Country", when(df1.col("Country") === df2.col("Country"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Quantity", when(df1.col("Quantity") === df2.col("Quantity"), lit("Y")).otherwise(lit("N")))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
    result.show()

    val primaryKeyList = Seq("ID")
    val schemaSchemaList = df1.columns.toList
    val columnToSelect = schemaSchemaList diff primaryKeyList

    val joinResult = df1.as("df1").join(df2.as("df2"), col("df1.ID") === col("df2.ID"), "inner")
    joinResult.show()

    // val newDF2 = joinResult.select("df1.ID","df1.Product","df1.Country","df1.Quantity")
    // newDF2.show()

    val matchRec = columnToSelect.map((i => joinResult.withColumn((s"M_$i"), when(df1.col((s"$i")) === df2.col((s"$i")), lit("Y")).otherwise(lit("N")))))
    val newDF = matchRec.reduce((x, y) => x.join(y,"ID"))
    newDF.show()

    println("Present in Source not in Target:")
    val inZ = df1.except(df2)
    inZ.show()

    //val newDF1 = newDF.select("df1.ID","df1.Product","df1.Country","df1.Quantity")
   // newDF1.show()

    /*
    // val finalDF = newDF.as("newDF").join(joinResult.as("joinResult"), col("newDF.ID") === col("joinResult.ID"), "inner")
    // finalDF.show()

       val result = df1.join(df2, df1.col("ID") === df2.col("ID"))
      .withColumn("M_Product", when(df1.col("Product") === df2.col("Product"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Country", when(df1.col("Country") === df2.col("Country"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Quantity", when(df1.col("Quantity") === df2.col("Quantity"), lit("Y")).otherwise(lit("N")))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
    result.show()

    println("Matching Records:")
    val diffDF = df1.intersect(df2)
    diffDF.show()

    println("Present in Source not in Target:")
    val inZ1 = df1.exceptAll(df2).toDF()
    inZ1.show()



    println("Present in Target not in Source:")
    val inZ2  = df2.exceptAll(df1).toDF()
    inZ2.show()
     */

  }
}
