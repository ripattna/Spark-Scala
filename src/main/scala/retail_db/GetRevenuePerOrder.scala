package retail_db

import org.apache.spark.{SparkConf, SparkContext}

object GetRevenuePerOrder {

  def main(args: Array[String]): Unit = {

    // Create Conf Object and to initializing the SparkContext
    val conf = new SparkConf().setMaster(args(0)).setAppName("GetRevenuePerOrder")
    val sc = new SparkContext(conf)

    //Creating log level
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    sc.setLogLevel("WARN")

    // Reading the text file
    val orderItems = sc.textFile(args(1))

    val revenuePerOrder = orderItems.map(x => (x.split(",")(1).toInt,x.split(",")(4).toFloat)).
      reduceByKey(_ + _).map(x => x._1 + "," + x._2)

    // Saving the outputs to disk
    revenuePerOrder.saveAsTextFile(args(2))
  }
}
