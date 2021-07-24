package com.dataframe

import  org.apache.log4j.Level
import  org.apache.log4j.Logger
import  org.apache.spark.SparkConf
import  org.apache.spark.sql.SparkSession
import  scala.collection.mutable.Map
import org.apache.spark.sql.types.{StructType,DoubleType,IntegerType,StringType}
import  org.apache.spark.sql.functions.{year,month,to_date,col,asc,desc,sum,count,date_format,lit,when,avg,round}

object  DataframeAvgOrdersPerMonth  extends  App{ Logger.getLogger("org").setLevel(Level.ERROR)
  /**
   *  Spark  Settings  and  Session  Creation
   */

  val  sparkConf  =  new  SparkConf()
  sparkConf.set("spark.app.name",  "Dataframe  Avg  Orders  per  Month")
  sparkConf.set("spark.master",  "local[2]")

  val  spark  =  SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  /**
   *	external  schema  for  the  data
   */

  val  orderSchema  =  new  StructType()
    .add("Date",StringType)
    .add("Price",DoubleType)
    .add( "Quantity" , IntegerType)
    .add(" cust_id", IntegerType)
    .add(" order_id", IntegerType)
/** reading the data from XML file and provide t he schema for the data **/

  val ordersData = spark.read.format(" xml")
  .option(" rootTag", " dataset")
  .option(" rowTag", " record")
  .schema(orderSchema)
  .load(" C:\\BIGDATA\\Spark\\dataset.xml")

/**
 * change the date datatype from String t o Date format * */
val d ateFormatChangedDf = ordersData.withColumn(" Date",
to_date(col(" Date") ," dd/MM/yyyy") )
/**
 * get the year and month values from the Date
 * /
                                                          ````
val yearandMonthDf = d ateFormatChangedDf.withColumn(" Year",
year(col(" Date") ))
.withColumn(" Month", month(col(" Date") ))
.drop(" Date")
/**
 * Group by Y ear and Month column
 * product o f Quantity a nd P rice columns t hen S um a s t otal_price
 * aggregate on total_price, customer id
 * /
val group_and_AggDf =
yearandMonthDf.groupBy(col(" Year") ,col(" Month") )
.agg(sum(col( "Price" )*col( "Quantity" )).as( "total_price" ),count( "cust_id" ).a
s(" customers") )
.orderBy(desc(" Year") ,asc(" Month") , d esc(" total_price") )
.withColumn(" Month", when(col(" Month") .equalTo(1 ) , " Jan")
.when(col(" Month") .equalTo(2 ) , " Feb")
.when(col(" Month") .equalTo(3 ) , " Mar")
.when(col(" Month") .equalTo(4 ) , " Apr")
.when(col(" Month") .equalTo(5 ) , " May")
