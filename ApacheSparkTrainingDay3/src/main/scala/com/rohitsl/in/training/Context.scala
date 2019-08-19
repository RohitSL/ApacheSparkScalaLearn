package com.rohitsl.in.training
import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
trait Context {
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  lazy val sparkConf = new SparkConf()
    //.setMaster("spark://localhost:7077")
    .setMaster("local[*]")
    //.set("spark.cores.max", "1")
    .set("spark.sql.warehouse.dir", warehouseLocation)
    .set("spark.driver.bindAddress", "127.0.0.1")

  val sparkSession = SparkSession
    .builder()
    .appName("mySparkApp")
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

}