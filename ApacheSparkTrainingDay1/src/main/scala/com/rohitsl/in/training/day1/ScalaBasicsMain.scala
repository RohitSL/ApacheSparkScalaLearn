package com.rohitsl.in.training.day1

//import com.microsoft.in.training.Context

//object ScalaBasicsMain {
//  def main(args:Array[String]):Unit ={
//    println("Hello world! How are you today?")
//    println(args.mkString(", "))
//  }
//}

//object ScalaBasicsMain extends App {
//  println("Hello world! How are you today?")
//  println(args.mkString(", "))
//}

object ScalaBasicsMain
{
  def main(args:Array[String]):Unit ={
    val basics:ScalaLanguageBasics = new ScalaLanguageBasics()
    basics.HelloWorld()
    basics.ImmutableVariables()
    basics.MutableVariables()
    System.out.println(basics.LazyInitialization())
    basics.SupportedDataTypes()
    basics.InitializeWithoutValue()
    basics.StringInterpolation()
    basics.StringInterpolationFormatted()
    basics.MultiLineString()
    basics.TypeInference()
    basics.TypeConversion()
    basics.IfElseElseIfExample()
    basics.ForLoop()
    basics.ForLoopContinued()
    basics.ForLoop2DimArray()
    basics.RangeExamples()
    basics.RangeToCollection()
    basics.WhileLoop()
    basics.MatchExample()
    basics.MatchTypesExample()
    basics.TuplesExample()
    basics.OptionExample()
    basics.AnyExample()
    basics.EnumExample()
    basics.EnumWithChangedOrder()
  }
}

