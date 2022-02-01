package com.automation_2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

/**
 * Contains comparison related operations
 */
class CompareTwoDF {

  // Spark Session
  val spark = SparkSession.builder().master("local").appName("CompareTwoDF").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  /**
   * Read Source & Target File and convert to Spark DataFrame
   * File could be in S3 , HDFS or local file system
   * For example, for HDFS, "hdfs://nn1home:8020/input/war-peace.parquet"
   * For S3 location, "s3n://myBucket/myFile1.csv"
   * @param fileFormat format of the file type it could be csv,parquet,text,json
   * @param fileLocation location of file located in any of the storage
   * @return a DataFrame
   */
  def readFile(fileFormat: String, fileLocation: String): DataFrame ={
    val df = spark.read.option("header", "true").option("inferSchema", "true").format(fileFormat).load(fileLocation)
    return df
  }

  /**
   * Can compare two files whether S3 , HDFS , local file system
   * For example, for HDFS, "hdfs://nn1home:8020/input/war-peace.parquet"
   * For S3 location, "s3n://myBucket/myFile1.csv"
   * @param sourceDF
   * @param targetDF
   * @param primaryKey
   * @param columns
   * @return  DataFrame
   */
  def compareResult(sourceDF: DataFrame, targetDF: DataFrame, primaryKey: List[String],  columns: List[String]): DataFrame = {

    val joinResult = sourceDF.as("sourceDF").join(targetDF.as("targetDF"), primaryKey, "left")
    val compResult = columns.foldLeft(joinResult) { (df, name) =>
      df.withColumn("M_" + name, when(col("sourceDF." + name) === col("targetDF." + name), lit("Y"))
        .otherwise(lit("N")))}
      .withColumn("MATCHING", when(col("M_Product") === "Y" && col("M_Country") === "Y" && col("M_Quantity") === "Y", lit("Y"))
        .otherwise(lit("N")))
    val compRes = columns.foldLeft(joinResult) { (df, name) => df.withColumn(name + "_M", when(col("sourceDF." + name) =!= col("targetDF." + name), lit(name))) }
      .withColumn("MissMatch_Column", concat_ws(",", columns.map(name => col(name + "_M")): _*))
    val resultDF = compRes.join(compResult, primaryKey, "inner")
      .select(col("sourceDF.*"), col("MATCHING"), col("MissMatch_Column"))

    return resultDF

    /**
    //.select(sourceDF.columns.map(x => sourceDF(x)): _*)
    //val res = compResult.select(compResult.columns.filter(_.startsWith("M_")).map(compResult(_)):_*)
    //val source = compResult.select(sourceDF.columns.map(x => sourceDF(x)): _*)
    //val rf = compResult.filter(col("sourceDF.*").notEqual("") && col("group").notEqual(""))
    // val compResult = columns.map(i => joinResult.withColumn((s"M_$i"), when(sourceDF.col((s"$i")) === targetDF.col((s"$i")), lit("Y")).otherwise(lit("N")))).reduce((x, y) => x.join(y,(primaryKey)))
     */
  }

  /**
   * Can compare two files whether S3 , HDFS , local file system
   * For example, for HDFS, "hdfs://nn1home:8020/input/war-peace.parquet"
   * For S3 location, "s3n://myBucket/myFile1.csv"
   * @param sourceDF
   * @param targetDF
   * @return  DataFrame
   */
  def matchRecords(sourceDF: DataFrame, targetDF: DataFrame): DataFrame ={
    val matchRes = sourceDF.intersect(targetDF)
    matchRes
  }

  /**
   * Can compare two files whether S3 , HDFS , local file system
   * For example, for HDFS, "hdfs://nn1home:8020/input/war-peace.parquet"
   * For S3 location, "s3n://myBucket/myFile1.csv"
   * @param sourceDF
   * @param targetDF
   * @return  DataFrame
   */
  def mismatchRecords(sourceDF: DataFrame, targetDF: DataFrame): DataFrame ={
    val mismatchSourceDF = sourceDF.exceptAll(targetDF).toDF()
    return mismatchSourceDF
  }

}
object CompareTwoDFObject {

  def main(args: Array[String]): Unit = {

    // Reading the conf file
    val applicationConf: Config = ConfigFactory.load("Config/app.conf")
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

    println("Final DataFrame:")
    val comRes = new CompareTwoDF().compareResult(sourceDF, targetDF, primaryKeyList, columnToSelect)
    comRes.show(false)

    /*
    println("Matching Records:")
    val matchRes = new CompareTwoDF().matchRecords(sourceDF, targetDF)
    matchRes.show()

    println("Mismatch Rows in Source:")
    val sourceMismatchRecords = new CompareTwoDF().mismatchRecords(sourceDF, targetDF)
    sourceMismatchRecords.show()

    println("Mismatch Rows in Target:")
    val targetMismatchRecords = new CompareTwoDF().mismatchRecords(targetDF, sourceDF)
    targetMismatchRecords.show()
     */

  }
}