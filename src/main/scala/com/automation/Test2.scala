package com.automation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Test1").master("local").getOrCreate()
    // Creating log level
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val seq = Seq((1,100,0,0,0,0,0),(2,0,50,0,0,20,0),(3,0,0,0,0,0,0),(4,0,0,0,0,0,0))
    val df1 = seq.toDF("segment_id", "val1", "val2", "val3", "val4", "val5", "val6")
    df1.show()

    val schema = df1.schema

    val df2 = df1.flatMap(row => {
      val metric = row.getInt(0)
      (1 until row.size).map(i => {(metric, schema(i).name, row.getInt(i))})})

    val df3 = df2.toDF("metric", "vals", "value")
    df3.show()

    val df4 = df3.groupBy("vals").pivot("metric").agg(first("value"))
    df4.show()

  }

}
