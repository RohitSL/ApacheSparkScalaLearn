package com.rohitsl.in.training.day2


import com.rohitsl.in.training.Context

object SparkRDDMain extends App with Context{
  val rddBasics:SparkRDDBasics = new SparkRDDBasics()
    rddBasics.RDDCreate()
    rddBasics.RDDTransform()
    rddBasics.RDDAction()
    rddBasics.RDDPersistence()
    rddBasics.RDDSpecials()
}
