package com.rohitsl.in.training

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkConf = new SparkConf()
    .setAppName("My Test Application")
    //.setMaster("spark://localhost:7077")
    //.config("spark.sql.warehouse.dir", "warehouseLocation")
    //.enableHiveSupport()
    .setMaster("local[1]")
    .set("spark.cores.max", "1")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
}