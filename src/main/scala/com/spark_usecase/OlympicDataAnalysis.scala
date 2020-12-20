package com.spark_usecase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object  OlympicDataAnalysis {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder().appName("OlympicDataAnalysis").master("local").getOrCreate()

    // Creating log level
    spark.sparkContext.setLogLevel("WARN")

    // Reading the csv file
    val df = spark.read.option("delimiter",",").option("inferSchema", "true").option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .schema("Name string ,Age integer ,Country string ,Year string ,Closing_Date string ,Sport string ,Gold_Medals integer ,Silver_Medals integer ,Bronze_Medals integer ,Total_Medals integer")
      .csv("C:\\Project\\Files\\Input\\csv\\olympics_data.csv")

    df.createGlobalTempView("olympic")
    val sqlDF = spark.sql("select * from global_temp.olympic limit 5").show()

    // Find the total number of medals won by each country in swimming
    val swimWiseMedal = spark.sql("select country,sport,sum(total_medals) from global_temp.olympic where sport='Swimming' group by sport,country ").show()

    // Find the total number of medals tht India won year wise.
    val IndiaWon = spark.sql("select country,year,sum(total_medals) from global_temp.olympic where country='India' group by country,year order by year desc").show()

    // Find the total numbers of medals won by each country
    val countryWiseMedal = spark.sql("select country,sum(total_medals) from global_temp.olympic group by country").show()

    // Fin the total medal won by Pakistan
    val panWonMedal = spark.sql("select country,sum(total_medals) from global_temp.olympic where country='Indonesia' group by country").show()

  }

}