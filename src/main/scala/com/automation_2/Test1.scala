package com.automation_2

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

object Test1 {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("ReconAutomation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    def random(n: Int, s: Long) = {
    spark.range(n).select(
      (rand(s) * 10000).cast("int").as("a"),
      (rand(s + 5) * 1000).cast("int").as("b"))}

    val df1 = random(10000000, 34)
    df1.show()
    val df2 = random(10000000, 17)
    df2.show()

  // Move all the keys into a struct (to make handling nulls easy), deduplicate the given dataset
  // and count the rows per key.
  def deDup(df: Dataset[Row]): Dataset[Row] = {
    df.select(struct(df.columns.map(col): _*).as("key"))
      .groupBy("key")
      .agg(count(lit(1)).as("row_count"))
  }

    //val joinDF = deDup(df1).as("l").join(deDup(df2).as("r"), "l.key" === "r.key", "full")
    val joined = deDup(df1).as("l").join(deDup(df2).as("r"), Seq("key"), "full")
    joined.show()


  }

}


