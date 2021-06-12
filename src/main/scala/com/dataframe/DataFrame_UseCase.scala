package com.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}

object DataFrame_UseCase extends App {
  // Spark Session
  val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
  //Handling the log
  spark.sparkContext.setLogLevel("ERROR")

  // Creating a sample dataset
  val columns = Seq("Dept","Emp")
  val data = List(("X","a"),("X","a"),("Y","b"))

  // Create DataFrame from the data
  import spark.implicits._
  val df_emp = data.toDF(columns:_*)
  df_emp.show()

  val df_hd = df_emp.withColumn("DeptHeadCount",
     when (col("Dept")==="X",df_emp.where("Dept = 'X'").groupBy("Dept").count().select(col("count").cast("string"))
       .head.getString(0))
       when (col("Dept")==="Y",df_emp.where("Dept = 'Y'").groupBy("Dept").count().select(col("count").cast("string"))
      .head.getString(0)))

  df_hd.show()

  println("########################################################")

  val a = Seq(("x","a"),("x","a"),("y","b")).toDF("dept","emp")
  a.show()
  val b = a.select("dept").groupBy("dept").count()
  b.show()
  val c = a.join(b, a("dept") === b("dept"),"inner").drop(b("dept"))
    .withColumnRenamed("count","DeptHeadCount")
    .select(col("dept"),col("emp"),col("DeptHeadCount"))

  c.show()

}
