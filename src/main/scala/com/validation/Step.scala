package com.validation

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

class Step {

  // Reading the conf file
  val applicationConf: Config = ConfigFactory.load("config.conf")

  // Reading the Spark Environment
  val masterEnv: String = applicationConf.getString("sparkEnvironment.master")
  val appName: String = applicationConf.getString("sparkEnvironment.appName")

  // Spark Session
  val spark = SparkSession.builder().master(masterEnv).appName(appName).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

}
