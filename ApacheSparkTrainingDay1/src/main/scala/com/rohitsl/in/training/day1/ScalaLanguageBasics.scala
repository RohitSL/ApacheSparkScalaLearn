package com.rohitsl.in.training.day1

class ScalaLanguageBasics {

  def HelloWorld():Unit={
    println("Hello World! How are you today?")
  }

  def ImmutableVariables():Unit={
    //val <VariableName>: <Scala Data Type> = <Literal Value>
    val apples: Int = 5
    //apples = 10 //Error
  }

  def MutableVariables():Unit={
    var favoriteFruit: String = "Mango"
    favoriteFruit = "Apple" //No Error
  }

  def LazyInitialization():String={
    lazy val juiceOf = "Orange"
    juiceOf //This returns the value of juiceOf this is when this statement is evaluated
  }

  def SupportedDataTypes():Unit={
    val apples: Int = 5
    val fruitsInMarket: Long = 100000000L
    val fruitsInBasket: Short = 1
    val priceOfMango: Double = 2.50
    val priceOfOrange: Float = 2.50f
    val nameOfFruit: String = "Mango"
    val mangoByte: Byte = 0xa
    val mangoStartsWith: Char = 'M'
    val nothing: Unit = ()
  }

  def InitializeWithoutValue():Unit={
    //var leastFavoriteFruit: String = _   //   _ is wildcard or place holder operator. IDE complains
    //leastFavoriteFruit = "Raw mango"
  }

  def StringInterpolation():Unit={
    val favoriteFruit: String = "Mango"
    println(s"My favorite fruit = $favoriteFruit")//Prints the string on console

    //String interpolation of object properties
    case class Fruit(name: String, taste: String)
    val anotherFavoriteFruit: Fruit = Fruit("Orange", "Sweet")
    println(s"My another favorite fruit name = ${anotherFavoriteFruit.name}, taste = ${anotherFavoriteFruit.taste}")

    //String interpolation for expressions
    val qtyFruits: Int = 10
    println(s"Are we buying 10 mangoes = ${qtyFruits == 10}")

  }

  def StringInterpolationFormatted():Unit = {
    val dryFruitName = "Almond"
    //Formatting interpolation
    val dryFruitPrice: Double = 500.50
    println(s"DryFruit price = $dryFruitPrice")
    println(f"Formatted DryFruit price = $dryFruitPrice%.2f")

    //Raw interpolation
    println(raw"Favorite DryFruit\t$dryFruitName")
  }

  def MultiLineString():Unit={
    val fruitLabel: String =
      """
        |{
        |"fruit":"Mango",
        |"state":"Ripe",
        |"price":8.50
        |}
      """.stripMargin
    println(s"FruitLabel = $fruitLabel")
    //Different separator #
    val fruitLabel2: String =
      """
      #{
      #"fruit":"Mango",
      #"state":"Raw",
      #"price":3.00
      #}
      """.stripMargin('#')

    println(s"FruitLabel = $fruitLabel2")

  }

  def TypeInference():Unit={
    //val <VariableName> : <Scala data type> = <Literal value>
    val apples = 5
    val fruitsInMarket = 100000000L
    val fruitsInBasket = 1
    val priceOfMango = 2.50
    val priceOfOrange = 2.50f
    val nameOfFruit = "Mango"
    val mangoByte = 0xa
    val mangoStartsWith = 'M'
    val nothing = ()

  }

  def TypeConversion():Unit ={
    val mangoesInBasket: Short = 1
    val mangoesToEat: Int = mangoesInBasket

    //val giveMangoes: String = mangoesInBasket //This fails

    val giveMeMangoes: String = mangoesInBasket.toString() //works

  }

  def IfElseElseIfExample(): Unit ={
    val mangoes = 20
    val mangoesPerBasket = 10
    val minimumMangoes = 8
    if(mangoes > 10)
      println(s"Number of mango baskets = ${mangoes / mangoesPerBasket}")
    else if (mangoes == 10)
      println(s"Number of mango baskets = 1")
    else
      println(s"Number of mangoes = $minimumMangoes")

    //Usagae like ternary operator <condition>? if true: if false
    val baskets = if(mangoes > 10) (mangoes / mangoesPerBasket) else 1
    println(s"Number of mango baskets = $baskets")
  }

  def ForLoop(): Unit = {
    //A simple for loop from 1 to 5 inclusive
    for(mangoes <- 1 to 5){
      println(s"Number of mangoes = $mangoes")
    }

    //A simple for loop from 1 to 5, where 5 is NOT inclusive
    for(mangoes <- 1 until 5){
      println(s"Number of mangoes = $mangoes")
    }
    //Filter values using if conditions in for loop
    val mangoVarieties = List("Alphonso", "Dasheri", "Badami", "Himsagar", "Fazli")
    for(variety <- mangoVarieties if variety == "Alphonso" || variety == "Dasheri"){
      println(s"Found best mangoes = $mangoVarieties")
    }
  }

  def ForLoopContinued():Unit={
    val mangoVarieties = List("Alphonso", "Dasheri", "Badami", "Himsagar", "Fazli")
    val sweetMangoes = for { variety <- mangoVarieties if (variety == "Alphonso" || variety == "Dasheri") }
        yield variety
    //Note: This time it is curly braces in for loop declaration as we are using yield
    println(s"Sweetening ingredients = $sweetMangoes")

  }

  def ForLoop2DimArray():Unit={
    val twoDimArr = Array.ofDim[String](2,2)
    twoDimArr(0)(0) = "Alphonso"
    twoDimArr(0)(1) = "Dasheri"
    twoDimArr(1)(0) = "Badami"
    twoDimArr(1)(1) = "Fazli"

    for { x <- 0 until 2; y <- 0 until 2 }
      println(s"Mango at index ${(x,y)} = ${twoDimArr(x)(y)}")

  }

  def RangeExamples():Unit ={
    //Create a simple numeric range from 1 to 5 inclusive
    val oneToFive = 1 to 5
    println(s"Range from 1 to 5 inclusive = $oneToFive")
    //Create a numeric range from 1 to 5 but excluding the last integer number 5
    val oneUntilFive = 1 until 5
    println(s"Range from 1 until 5 where 5 is excluded = $oneUntilFive")
    //Create a numeric range from 0 to 10 but increment with multiples of 2
    val zeroToTenBy2 = 0 to 10 by 2
    println(s"Range from 0 to 10 with multiples of 2 = $zeroToTenBy2")
    //Create an alphabetical range to represent letter a to z
    val aToz = 'a' to 'z'
    println(s"Range of alphabets from a to z = $aToz")

    aToz.foreach(println) //To print the list

    //Character ranges with user specified increment
    val aTozBy2 = 'a' to 'z' by 2
    println(s"Range of every other alphabet = $aTozBy2")

  }

  def RangeToCollection():Unit={
    val listOneToFive = (1 to 5).toList
    println(s"Range to list = ${listOneToFive.mkString(" ")}")
    listOneToFive.foreach(print(_)) //also prints the list.

    val setOneToFive = (1 to 5).toSet
    println(s"Range to set = ${setOneToFive.mkString(" ")}")

    val seqOneToFive = (1 to 5).toSeq
    println(s"Range to sequence = ${seqOneToFive.mkString(" ")}")

    val arrOneToFive = (1 to 5).toArray
    println(s"Range to array = ${arrOneToFive.mkString(" ")}")
  }

  def WhileLoop():Unit={
    //How to use while loop in Scala
    var mangoes = 10
    while (mangoes > 0) {
      println(s"Remaining mangoes = $mangoes")
      mangoes -= 1 //Ate one mango
    }
    //How to use do while loop in Scala
    mangoes = 0
    do {
      mangoes += 1 //Pluck mango from tree
      println(s"Mangoes plucked = $mangoes")
    } while (mangoes < 5)

  }

  def MatchExample():Unit={
    val mangoVariety = "Alphonso"
    val taste = mangoVariety match {
      case "Alphonso" | "Badami" => "Very sweet"
      case mango if (mangoVariety.contains("Naturally Riped") || mangoVariety.contains("Organic")) => "Healthy"
      case "Fazli" => "Sweet"  //Ordinary case statement
      case _ => "Ok" //Default case
    }
    println(s"Taste of $mangoVariety = $taste")

  }

  def MatchTypesExample():Unit={
    val priceOfMango: Any = 10.00
    val typeOfPrice = priceOfMango match {
      case price: Int => "Int"
      case price: Double => "Double"
      case price: Float => "Float"
      case price: String => "String"
      case price: Boolean => "Boolean"
      case price: Char => "Char"
      case price: Long => "Long"
    }
    println(s"Mango price type = $typeOfPrice")

  }

  def TuplesExample():Unit={
    val mangoTaste = Tuple2("Alphonso", "Very Sweet")
    println(s"We have = $mangoTaste")

    val variety = mangoTaste._1
    val taste = mangoTaste._2
    println(s"$variety taste is $taste")

    val mangoTastePrice = Tuple3("Alphonso", "Very Sweet", 35.50) //Is same as
    val mangoTastePrice2 = ("Badami", "Sweet", 20.50) //No need to put Tuple3 due to type inference
    println(s"${mangoTastePrice._1} taste is ${mangoTastePrice._2} and it's price is ${mangoTastePrice._3}")

  }

  def OptionExample():Unit={
    val alphonsoTaste: Option[String] = Some("Very Sweet") //Option is equivalent to nullable but safe
    println(s"Alphonso taste = ${alphonsoTaste.get}") //Can result in null pointer exception if Some is not set
    val mangoName: Option[String] = None //Safely mark no value
    //mangoName.get()
    println(s"Mango name = ${mangoName.getOrElse("Don't Know")}")
  }

  def AnyExample():Unit={
    val favoriteMango: Any = "Alphonso"
    println(s"Favorite mango of type Any = $favoriteMango")
    val alphonsoMango: AnyRef = "Alphonso"
    println(s"Name of mango type AnyRef = $alphonsoMango")
    val alphonsoPrice: AnyVal = 35.50
    println(s"Alphonso price of type AnyVal = $alphonsoPrice")
  }

  def EnumExample():Unit={
    object Mango extends Enumeration {
      type Mango = Value

      val Alphonso = Value("Alphonso")
      val Badami = Value("Badami")
      val Himsagar = Value("Himsagar")
      val Fazli = Value("Fazli")
    }

    println(s"Alphonso mango string value = ${Mango.Alphonso}")
    println(s"Alphonso mango's id = ${Mango.Alphonso.id}")
    println(s"Mango types = ${Mango.values}")

  }

  def EnumWithChangedOrder():Unit={
    object MangoTaste extends Enumeration{
      type MangoTaste = Value
      val Sweet = Value(0, "Sweet")
      val VerySweet = Value(1, "Very Sweet")
      val Ok = Value(-1, "Ok")
    }
    println(s"Mango taste values = ${MangoTaste.values}")
    println(s"Mango taste of OK id = ${MangoTaste.Ok.id}")
  }
}
