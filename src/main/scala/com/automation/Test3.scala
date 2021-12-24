package com.automation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first

object Test3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test3").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._

    val df = Seq(
      ("Sam", "6th Grade", "Maths", "A"),
      ("Sam", "6th Grade", "Science", "A"),
      ("Sam", "7th Grade", "Maths", "A-"),
      ("Sam", "7th Grade", "Science", "A"),
      ("Rob", "6th Grade", "Maths", "A"),
      ("Rob", "6th Grade", "Science", "A-"),
      ("Rob", "7th Grade", "Maths", "A-"),
      ("Rob", "7th Grade", "Science", "B"),
      ("Rob", "7th Grade", "AP", "A")).toDF("Student", "Class", "Subject", "Grade")
   df.show()

    df.groupBy("Student", "Class").pivot("Subject").agg(first("Grade")).
      orderBy("Student", "Class").show()

  }
}
