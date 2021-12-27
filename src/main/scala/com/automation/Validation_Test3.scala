package com.automation

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object Validation_Test3 {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    // Creating log level
    spark.sparkContext.setLogLevel("ERROR")

    // Read the source file
    val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Project\\Files\\df1.csv")
    println("Printing the First DF:")
    df1.show()

    // Read the target file
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Project\\Files\\df2.csv")
    println("Printing the Second DF:")
    df2.show()

    println("For No of Rows in Source where value is 1:")
    // For No of Rows in Source where value is 1
    val source_data = df1.agg(sum("valueA"), sum("valueB"), sum("valueC"),
      sum("valueD"))
      .select(col("sum(valueA)").as("valueA"), col("sum(valueB)").as("valueB"),
        col("sum(valueC)").as("valueC"), col("sum(valueD)").as("valueD"))
    source_data.show()

    println("For No of Rows in Target where value is 1:")
    // For No of Rows in Target where value is 1
    val target_data = df2.agg(sum("valueA"), sum("valueB"), sum("valueC"),
      sum("valueD"))
      .select(col("sum(valueA)").as("valueA"), col("sum(valueB)").as("valueB"),
        col("sum(valueC)").as("valueC"), col("sum(valueD)").as("valueD"))
    target_data.show()

    source_data.createOrReplaceTempView("axis")
    val b = spark.sqlContext.sql("select 'valueA' as Column_Name,valueA as No_Of_Rows_Source from axis union " +
      "select 'valueB', valueB  from axis union " + "select 'valueC', valueC  from axis union " + "select 'valueD', valueD  from axis")
    // b.show()

    target_data.createOrReplaceTempView("axis")
    val b1 = spark.sqlContext.sql("select 'valueA' as Column_Name,valueA as No_Of_Rows_Target from axis union " +
      "select 'valueB', valueB  from axis union " + "select 'valueC', valueC  from axis union " + "select 'valueD', valueD  from axis")
    // b1.show()

    val join_data = b.join(b1, b("Column_Name") === b1("Column_Name"), "inner").drop(b1("Column_Name")).orderBy("Column_Name")
    //join_data.show()

    // Inner join to get the Overlap Count

    println("Join result for valueA")
    val valueA_1 = df1.join(df2, Seq("Primary_Key","valueA")).agg(sum("valueA")).select(col("sum(valueA)").as("valueA"))
    println(valueA_1.count())

    println("Join result for valueB")
    val valueB_1 = df1.join(df2, Seq("Primary_Key","valueB")).agg(sum("valueB")).select(col("sum(valueB)").as("valueB"))
    println(valueB_1.count())

    println("Join result for valueC")
    val valueC_1 = df1.join(df2, Seq("Primary_Key","valueC")).agg(sum("valueC")).select(col("sum(valueC)").as("valueC"))
    println(valueC_1.count())

    println("Join result for valueD")
    val valueD_1 = df1.join(df2, Seq("Primary_Key","valueD")).agg(sum("valueD")).select(col("sum(valueD)").as("valueD"))

    println(valueD_1.count())

    val df11 = spark.sqlContext.createDataFrame(valueA_1.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueA_1.schema.fields :+ StructField("index", LongType, false)))
    val df22 = spark.sqlContext.createDataFrame(valueB_1.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueB_1.schema.fields :+ StructField("index", LongType, false)))
    val df33 = spark.sqlContext.createDataFrame(valueC_1.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueC_1.schema.fields :+ StructField("index", LongType, false)))
    val df44 = spark.sqlContext.createDataFrame(valueD_1.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueD_1.schema.fields :+ StructField("index", LongType, false)))

    val join1 = df11.join(df22, Seq("index"))
    val join2 = df33.join(df44, Seq("index"))
    val join_df = join1.join(join2,Seq("index")).drop("index")
    println("Inner join result:")
    join_df.show()

    join_df.createOrReplaceTempView("axis")
    val b01 = spark.sqlContext.sql("select 'valueA' as Column_Name,valueA as Overlap_Count from axis union " +
      "select 'valueB', valueB  from axis union " + "select 'valueC', valueC  from axis union " + "select 'valueD', valueD  from axis")
    println("Inner join Transpose:")
    b01.show()

    val final_df1 = join_data.join(b01,Seq("Column_Name"))
    final_df1.show()

    /*
    val valueA_12 = df1.join(df2, Seq("Primary_Key","valueA"), "leftanti").agg(sum("valueA")).select(col("sum(valueA)").as("valueA"))
    val valueB_12 = df1.join(df2, Seq("Primary_Key","valueB"),"leftanti").agg(sum("valueB")).select(col("sum(valueB)").as("valueB"))
    val valueC_12 = df1.join(df2, Seq("Primary_Key","valueC"),"leftanti").agg(sum("valueC")).select(col("sum(valueC)").as("valueC"))
    val valueD_12 = df1.join(df2, Seq("Primary_Key","valueD"),"leftanti").agg(sum("valueD")).select(col("sum(valueD)").as("valueD"))

    val df011 = spark.sqlContext.createDataFrame(valueA_12.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueA_12.schema.fields :+ StructField("index", LongType, false)))
    val df022 = spark.sqlContext.createDataFrame(valueB_12.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueB_12.schema.fields :+ StructField("index", LongType, false)))
    val df033 = spark.sqlContext.createDataFrame(valueC_12.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueC_12.schema.fields :+ StructField("index", LongType, false)))
    val df044 = spark.sqlContext.createDataFrame(valueD_12.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueD_12.schema.fields :+ StructField("index", LongType, false)))

    val join01 = df011.join(df022, Seq("index"))
    val join02 = df033.join(df044, Seq("index"))
    val join_df0 = join01.join(join02,Seq("index")).drop("index")
    println("Present in Source not in Target:")
    join_df0.show()

    join_df0.createOrReplaceTempView("axis")
    val sb01 = spark.sqlContext.sql("select 'valueA' as Column_Name,valueA as Extra_Records_Source_V_1 from axis union " +
      "select 'valueB', valueB  from axis union " + "select 'valueC', valueC  from axis union " + "select 'valueD', valueD  from axis")
    sb01.show()

    val final_df_s = final_df1.join(sb01,Seq("Column_Name")).orderBy("Column_Name")

    val valueA_21 = df2.join(df1, Seq("Primary_Key","valueA"), "leftanti").agg(sum("valueA")).select(col("sum(valueA)").as("valueA"))
    val valueB_21 = df2.join(df1, Seq("Primary_Key","valueB"),"leftanti").agg(sum("valueB")).select(col("sum(valueB)").as("valueB"))
    val valueC_21 = df2.join(df1, Seq("Primary_Key","valueC"),"leftanti").agg(sum("valueC")).select(col("sum(valueC)").as("valueC"))
    val valueD_21 = df2.join(df1, Seq("Primary_Key","valueD"),"leftanti").agg(sum("valueD")).select(col("sum(valueD)").as("valueD"))

    val df911 = spark.sqlContext.createDataFrame(valueA_21.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueA_21.schema.fields :+ StructField("index", LongType, false)))
    val df922 = spark.sqlContext.createDataFrame(valueB_21.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueB_21.schema.fields :+ StructField("index", LongType, false)))
    val df933 = spark.sqlContext.createDataFrame(valueC_21.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueC_21.schema.fields :+ StructField("index", LongType, false)))
    val df944 = spark.sqlContext.createDataFrame(valueD_21.rdd.zipWithIndex.map { case (row, index) => Row.fromSeq(row.toSeq :+ index)}, StructType(valueD_21.schema.fields :+ StructField("index", LongType, false)))

    val join901 = df911.join(df922, Seq("index"))
    val join902 = df933.join(df944, Seq("index"))
    val join_df10 = join901.join(join902,Seq("index")).drop("index")
    println("Present in Source not in Target:")
    join_df10.show()

    join_df10.createOrReplaceTempView("axis")
    val tb01 = spark.sqlContext.sql("select 'valueA' as Column_Name,valueA as Extra_Records_Target_V_1 from axis union " +
      "select 'valueB', valueB  from axis union " + "select 'valueC', valueC  from axis union " + "select 'valueD', valueD  from axis")
    tb01.show()

    println("Final Result:")
    val final_df = final_df_s.join(tb01,Seq("Column_Name")).orderBy("Column_Name")
    final_df.show()

     */

  }

}
