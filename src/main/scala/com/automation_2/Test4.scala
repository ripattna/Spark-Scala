package com.automation_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, concat_ws, lit, when}

object Test4 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test3").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/Data/File_1.csv")
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("src/main/resources/Data/File_2.csv")

    // Joining two DF
    val columns = df1.columns
    val joinResult = df1.alias("d1").join(df2.alias("d2"), col("d1.ID") === col("d2.ID"), "left")

    // Return Missing Column Name
    val compRes = columns.foldLeft(joinResult) {(df, name) => df.withColumn(name + "_M", when(col("d1." + name) =!= col("d2." + name), lit(name)))}
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col(name + "_M")): _*))
    compRes.show()

    // Return Y or N
    val compResult = columns.foldLeft(joinResult) {(df, name) => df.withColumn("M_" + name, when(col("d1." + name) === col("d2." + name), lit("Y")).otherwise(lit("N")))}
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col("M_" + name)): _*))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
    //.select(col("d1.*"),col("MissMatch_Column"))
    compResult.show()

    // Return Y or N
    val compResult_ = columns.foldLeft(joinResult) {(df, name) => df.withColumn("M_" + name, when(col("d1." + name) === col("d2." + name), lit("Y")).otherwise(lit("N")))}
      //.withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col("M_" + name)): _*))
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
      .select(col("d1.*"),col("M_Product"),col("M_Country"),col("M_Quantity"),col("MATCHING"))
    compResult_.show()

    println("startsWith Expression output:")
    compResult.select(compResult.columns.filter(x => x.startsWith("M_")).map(x => col(x)):_*).columns


    /*
    // Return Y or N
    val compResult1 = columns.foldLeft(joinResult) {(df, name) => df.withColumn("M_" + name, when(col("d1." + name) === col("d2." + name), lit("Y")).otherwise(lit("N")))}
    compResult1.show()

    //Select columns by regular expression
    println("Regular Expression output:")
    compResult1.select(compResult1.colRegex("`^.*M*`")).show()

    println("startsWith Expression output:")
    compResult1.select(compResult1.columns.filter(f=>f.startsWith("M_")).map(m=>col(m)):_*).show()

    println("endWith Expression output:")
    compRes.select(compRes.columns.filter(f=>f.endsWith("_M")).map(m=>col(m)):_*).na.fill(0).show()

      val compRes = columns.foldLeft(joinResult) {(df, name) => df.withColumn(name + "_M", when(col("d1." + name) =!= col("d2." + name), lit(name)))}
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col(name + "_M")): _*))
    compRes.show()

      val compResult = columns.foldLeft(joinResult) {(df, name) => df.withColumn("M_" + name, when(col("d1." + name) === col("d2." + name), lit("Y")).otherwise(lit("N")))}
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y")).otherwise(lit("N")))
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col("M_" + name)): _*))
    compResult.show()
     */

  }

}
