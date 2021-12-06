package com.hive

import org.apache.spark.sql.SparkSession

object FutureXSparkTransformer {

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("Test")
      .master("local")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    val sampleSeq = Seq((1, "spark"), (2, "Big Data"))
    val df = spark.createDataFrame(sampleSeq).toDF("Course_Id", "Course_Name")
    df.show()

    df.write.format("csv").mode("overwrite").save("sample_sql")
    //df.write.mode("overwrite").saveAsTable("demo.sample_sql")

    // Creating TempView
    df.createOrReplaceTempView("Sample")
    println("Printing the Hive table value")
    spark.sql("select * from Sample").show()

  }

}
