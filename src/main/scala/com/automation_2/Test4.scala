package com.automation_2

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit, when}

object Test4 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test3").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/Data/File_1.csv")
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/Data/File_2.csv")

    val columns = df1.columns
    val joinResult = df1.alias("d1").join(df2.alias("d2"), col("d1.ID") === col("d2.ID"), "left")

    // Return Missing Column Name
    val compRes = columns.foldLeft(joinResult) {(df, name) => df.withColumn(name + "_M", when(col("d1." + name) =!= col("d2." + name), lit(name)))}
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col(name + "_M")): _*))
    compRes.show(false)

    // Return Y or N
    val compResult = columns.foldLeft(joinResult) {(df, name) => df.withColumn("M_" + name, when(col("d1." + name) === col("d2." + name), lit("Y")).otherwise(lit("N")))}
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col("M_" + name)): _*))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
    //.select(col("d1.*"),col("MissMatch_Column"))
    compResult.show()

    println("startsWith Expression output-1:")
    compResult.select(compResult.columns.filter(_.startsWith("M_")).map(compResult(_)):_*).show(false)

    println("startsWith Expression output-2:")
    compResult.select(compResult.columns.filter(x => x.startsWith("M_")).map(x => col(x)):_*).show(false)

    /*
        // Return Y or N
    val finalDF = columns.foldLeft(joinResult) {(df, name) => df.withColumn("M_" + name, when(col("d1." + name) === col("d2." + name), lit("Y")).otherwise(lit("N")))}
      //.withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col("M_" + name)): _*))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
      .select(col("d1.*"),col("M_Product"),col("M_Country"),col("M_Quantity"),col("MATCHING"))
    finalDF.show()
     */

  }

}
