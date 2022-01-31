package com.automation_2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import org.apache.spark.sql.functions._


class CompareTwoDF {

  // Spark Session
  val spark = SparkSession.builder().master("local").appName("CompareTwoDF").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // Read Source & Target File
  def readFile(fileFormat: String, path: String): DataFrame ={
    val df = spark.read.option("header", "true").option("inferSchema", "true").format(fileFormat).load(path)
    return df
  }

  def joinResult(sourceDF: DataFrame, targetDF: DataFrame, primaryKey: List[String], joinType: String, columns: List[String]): DataFrame ={
    val result = sourceDF.as("sourceDF").join(targetDF.as("targetDF"), primaryKey, joinType)
    return result
  }

  def compareResult(sourceDF: DataFrame, targetDF: DataFrame, primaryKey: List[String],joinType: String,columns: List[String]): DataFrame ={
    val JoinResult = sourceDF.as("sourceDF").join(targetDF.as("targetDF"), primaryKey, joinType)
    val compResult = columns.map(i => JoinResult.withColumn((s"M_$i"), when(sourceDF.col((s"$i")) === targetDF.col((s"$i")), lit("Y")).otherwise(lit("N"))))
      .reduce((x, y) => x.join(y,(primaryKey)))
    return compResult
  }

  def matchRecords(sourceDF: DataFrame, targetDF: DataFrame): DataFrame ={
    val matchRes = sourceDF.intersect(targetDF)
    matchRes
  }

  def mismatchRecordsInSource(sourceDF: DataFrame, targetDF: DataFrame): DataFrame ={
    val mismatchSourceDF = sourceDF.exceptAll(targetDF).toDF()
    return mismatchSourceDF
  }

  def mismatchRecordsInTarget(sourceDF: DataFrame, targetDF: DataFrame): DataFrame ={
    val mismatchSourceDF = targetDF.exceptAll(sourceDF).toDF()
    return mismatchSourceDF
  }

}
object CompareTwoDFObject {

  def main(args: Array[String]): Unit = {

    // Reading the conf file
    val applicationConf: Config = ConfigFactory.load("app.conf")
    // Reading the source and target file from config
    val sourcePath: String = applicationConf.getString("filePath.sourceFile")
    val targetPath: String = applicationConf.getString("filePath.targetFile")
    // Reading the file format from config
    val fileType: String = applicationConf.getString("fileFormat.fileType")
    // Reading the PrimaryKey from config
    val primaryKeyList = applicationConf.getStringList("primaryKey.primaryKeyValue").toList

    val sourceDF = new CompareTwoDF().readFile(fileType, sourcePath)
    println("Source Data:")
    sourceDF.show()

    val targetDF = new CompareTwoDF().readFile(fileType, targetPath)
    println("Target Data:")
    targetDF.show()

    // Schema of Source Data in List
    val schemaSchemaList = sourceDF.columns.toList
    // Columns to select after ignoring Primary Key
    val columnToSelect = schemaSchemaList diff primaryKeyList

    //val joinResult = new CompareTwoDF().joinResult(sourceDF, targetDF, primaryKeyList, "inner", columnToSelect)
    //joinResult.show()

    val comRes = new CompareTwoDF().compareResult(sourceDF, targetDF, primaryKeyList, "inner", columnToSelect)
    comRes.show()

    println("Matching Records:")
    val matchRes = new CompareTwoDF().matchRecords(sourceDF, targetDF)
    matchRes.show()

    println("Mismatch Rows in Source:")
    val sourceMismatchRecords = new CompareTwoDF().mismatchRecordsInSource(sourceDF, targetDF)
    sourceMismatchRecords.show()

    println("Mismatch Rows in Target:")
    val targetMismatchRecords = new CompareTwoDF().mismatchRecordsInTarget(sourceDF, targetDF)
    targetMismatchRecords.show()

  }
}