package com.automation_2

import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit, when}


object Test3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test3").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/Data/File_1.csv")
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/Data/File_2.csv")

    val result = df1.join(df2, df1.col("ID") === df2.col("ID"))
      .withColumn("M_ID", when(df1.col("ID") === df2.col("ID"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Product", when(df1.col("Product") === df2.col("Product"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Country", when(df1.col("Country") === df2.col("Country"), lit("Y")).otherwise(lit("N")))
      .withColumn("M_Quantity", when(df1.col("Quantity") === df2.col("Quantity"), lit("Y")).otherwise(lit("N")))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
    result.show(false)

    val columns = df1.columns
    val joinResult = df1.alias("d1").join(df2.alias("d2"), col("d1.ID") === col("d2.ID"), "left")

    val compResult = columns.foldLeft(joinResult) {(df, name) => df.withColumn("M_" + name, when(col("d1." + name) === col("d2." + name), lit("Y")).otherwise(lit("N")))}
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col("M_" + name)): _*))
    compResult.show()

    val compResult1 = columns.foldLeft(joinResult) {(df, name) => df.withColumn("M_" + name, when(col("d1." + name) === col("d2." + name), lit("Y")).otherwise(lit("N")))}
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col("M_" + name)): _*))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
    .select(col("d1.*"),col("MissMatch_Column"))
    compResult1.show()

    val compRes = columns.foldLeft(joinResult) {(df, name) => df.withColumn(name + "_M", when(col("d1." + name) =!= col("d2." + name), lit(name)))}
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col(name + "_M")): _*))
    compRes.show()

  }
}
