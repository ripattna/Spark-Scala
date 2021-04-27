package com.rdd

import org.apache.spark.sql.SparkSession

object RDDRepartitionExample extends App {

  val spark: SparkSession = SparkSession.builder().master("local[5]").appName("RDDRepartition").getOrCreate()
  /*
  //Creates empty RDD with no partition
  val rdd_1 = spark.sparkContext.emptyRDD// creates EmptyRDD[0]
  println("Partition Size:" +rdd_1.partitions.size)
  println("Num of Partitions: " +rdd_1.getNumPartitions) // returns o partition

  //Setting the rdd_1 partition to increase to 4 partition.
  val reparRdd = rdd_1.repartition(4) //Setting the partition to 4
  println("Re-partition count for rdd_1:"+reparRdd.getNumPartitions) //Outputs: "re-partition count:4

  //Setting the rdd_1 partition to increase to 2 partition.
  val reparRdd_1 = rdd_1.repartition(2) //Setting the partition to 2
  println("Re-partition count for rdd_1:"+reparRdd_1.getNumPartitions) //Outputs: "re-partition count:2

  val rdd = spark.sparkContext.parallelize(Range(0,20))
  //rdd.collect().foreach(println)
  //println("The RDD value is:"+ rdd)
  println("No of Partition:"+ rdd.getNumPartitions)
  //println("Partition Size:" + rdd.partitions.size)

 */

  val rdd1 = spark.sparkContext.parallelize(Range(0, 20), 6)
  println("parallelize : " + rdd1.partitions.size)
  rdd1.partitions.foreach(f => f.toString)

  val rddFromFile = spark.sparkContext.textFile("C:\\Project\\Files\\Input\\text/Input.txt", 9)
  //rddFromFile.collect().foreach(println)
  println("TextFile : " + rddFromFile.partitions.size)

  rdd1.saveAsTextFile("c:/tmp/partition")
  val rdd2 = rdd1.repartition(4)
  println("Repartition size : " + rdd2.partitions.size)
  rdd2.saveAsTextFile("c:/tmp/re-partition")

  val rdd3 = rdd1.coalesce(4)
  println("Repartition size : " + rdd3.partitions.size)

  rdd3.saveAsTextFile("c:/tmp/coalesce")
}
