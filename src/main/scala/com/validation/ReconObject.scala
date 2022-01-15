package com.validation

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object ReconObject{

  def main(args: Array[String]): Unit = {

    // Reading the conf file
    val applicationConf: Config = ConfigFactory.load("application.conf")

    // Reading the source and target file from config
    val sourcePath: String = applicationConf.getString("filePath.sourceFile")
    val targetPath: String = applicationConf.getString("filePath.targetFile")

    // Reading the file format from config
    val fileType: String = applicationConf.getString("fileFormat.fileType")

    // Reading the PrimaryKey from config
    val primaryKeyList = applicationConf.getStringList("primaryKey.primaryKeyValue").toList

    val sourceDF = new ReconValidation().readFile(fileType, sourcePath)
    sourceDF.show()
    val targetDF = new ReconValidation().readFile(fileType, targetPath)
    targetDF.show()

    // Schema of Source Data in List
    val schemaSchemaList = sourceDF.columns.toList
    // println(schemaSchemaList)

    // Columns to select after ignoring Primary Key
    val columnToSelect = schemaSchemaList diff primaryKeyList
    // println(columnToSelect)

    val sourceRowCount = new ReconValidation().rowsCount(sourceDF, columnToSelect)
    // sourceRowCount.show()
    val targetRowCount = new ReconValidation().rowsCount(targetDF, columnToSelect)
    // targetRowCount.show()

    // Overlap Records
    val overlapRowCount = new ReconValidation().joinDF("inner", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // overlapRowCount.show()
    // Extra Records in Source
    val extraSourceRowCount = new ReconValidation().joinDF("left_anti", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // extraSourceRowCount.show()
    val extraTargetRowCount = new ReconValidation().joinDF("left_anti",  columnToSelect, targetDF, sourceDF, primaryKeyList)
    // extraTargetRowCount.show()

    // Transpose the result
    val sourceRowsCount = new ReconValidation().TransposeDF(sourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","No_Of_Rec_Source")
    val targetRowsCount = new ReconValidation().TransposeDF(targetRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","No_Of_Rec_Target")
    val overlapRowsCount = new ReconValidation().TransposeDF(overlapRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Overlap_Count")
    val extraSourceRowsCount = new ReconValidation().TransposeDF(extraSourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Extra_Rec_Source")
    val extraTargetRowsCount = new ReconValidation().TransposeDF(extraTargetRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Extra_Rec_Target")

    // Final Result DF
    val finalDF = sourceRowsCount
      .join(targetRowsCount, Seq("Column_Name"),"inner")
      .join(overlapRowsCount, Seq("Column_Name"),"inner")
      .join(extraSourceRowsCount, Seq("Column_Name"),"inner")
      .join(extraTargetRowsCount, Seq("Column_Name"),"inner")
    finalDF.show()

    // Write DataFrame data to CSV file
    // finalDF.write.format("csv").option("header", true).mode("overwrite").save("/tmp/reconRes")


  }

}
