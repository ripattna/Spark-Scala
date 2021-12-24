package com.automation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, _}
//import org.apache.spark.sql.functions.col

object Validation_Test {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    // Creating log level
    spark.sparkContext.setLogLevel("ERROR")

    // Read the source file
    val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Project\\Files\\df1.csv")
    // df1.show()
    // Read the target file
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Project\\Files\\df2.csv")
    // df2.show()
    println("For No of Rows in Source where value is 1")
    // For No of Rows in Source where value is 1
    val source_data = df1.agg(sum("valueA"), sum("valueB"), sum("valueC"),
      sum("valueD"))
      .select(col("sum(valueA)").as("valueA"), col("sum(valueB)").as("valueB"),
        col("sum(valueC)").as("valueC"), col("sum(valueD)").as("valueD"))
    source_data.show()

    println("For No of Rows in Target where value is 1")
    // // For No of Rows in Target where value is 1
    val target_data = df2.agg(sum("valueA"), sum("valueB"), sum("valueC"),
      sum("valueD"))
      .select(col("sum(valueA)").as("valueA"), col("sum(valueB)").as("valueB"),
        col("sum(valueC)").as("valueC"), col("sum(valueD)").as("valueD"))
    target_data.show()

    source_data.createOrReplaceTempView("axis")
    val b = spark.sqlContext.sql("select 'valueA' as Column_Name," +
      "valueA as Source from axis union " +
      "select 'valueB', valueB  from axis union " +
      "select 'valueC', valueC  from axis union " +
      "select 'valueD', valueD  from axis")
    b.show()

    target_data.createOrReplaceTempView("axis")
    val b1 = spark.sqlContext.sql("select 'valueA' as Column_Name,valueA as Target from axis union " +
      "select 'valueB', valueB  from axis union " +
      "select 'valueC', valueC  from axis union " +
      "select 'valueD', valueD  from axis")
    b1.show()

    val join_data = b.join(b1, b("Column_Name") === b1("Column_Name"), "inner").drop(b1("Column_Name"))
    join_data.show()

    val join_data1 = df1.join(df2, df1("Primary_Key") === df2("Primary_Key"), "inner").drop(df2("Primary_Key"))
    println("Inner join result")
    //println(join_data1.count())

    // Using Join with multiple columns on where clause
    println("Join result for valueA")
    val valueA = df1.join(df2).where(df1("Primary_Key") === df2("Primary_Key") &&
      df1("valueA") === df2("valueA"))
    println(valueA.count())

    println("Join result for valueB")
    val valueB = df1.join(df2).where(df1("Primary_Key") === df2("Primary_Key") &&
      df1("valueB") === df2("valueB"))
    println(valueB.count())

    println("Join result for valueC")
    val valueC = df1.join(df2).where(df1("Primary_Key") === df2("Primary_Key") &&
      df1("valueC") === df2("valueC"))
    println(valueC.count())

    println("Join result for valueD")
    val valueD = df1.join(df2).where(df1("Primary_Key") === df2("Primary_Key") &&
      df1("valueD") === df2("valueD"))
    println(valueD.count())

    // Left Anti Join
    println("Left Anti join")
    val valueA_Anti = df1.join(df2).where(df1("Primary_Key") === df2("Primary_Key") && df1("valueA") === df2("valueA"))
    println(valueA_Anti.count())

    df1.join(df2,df1("Primary_Key") ===  df2("Primary_Key"),"left_anti").show(false)

    println("New Left Join Result")
    df1.join(df2,df1("Primary_Key") === df2("Primary_Key") &&  df1("valueA") === df2("valueA"), "left_anti").show()

    //  println("Present in Source but not present in target")

    // df1.except(df2).show(false)

    // println("Present in Target but not present in target")
    // df2.except(df1).show(false)

  }

}
