package com.rohitsl.in.training.day3
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.rohitsl.in.training.day3.SparkDFBasicsMain.sparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import sparkSession.implicits._

class SparkDataFrameBasics {

  def DFCreate(): Unit = {
    val inputPath = s"${System.getProperty("user.dir")}".replace("\\", "/") //Current working directory

    /** toDF method - Recommended for terse code */
    val someDF1 = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    ).toDF("number", "word")

    someDF1.show()


    /** CreateDataFrame method - Recommended for schema customization */
    //Step 1: load data in a Seq collection as rows
    val someData = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse"),
      Row(10, "goat")
    )
    //Step 2: Load schema in a List using StructField
    val someSchema = List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true)
    )

    //Step 3: Use createDataFrame and pass data and schema to get Data frame.
    val someDF2 = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(someData), StructType(someSchema)
    )

    /** Using Daria Custom wrapper on createDataFrame the above createDataFrame code is much simplified */
    val someDF3 = sparkSession.createDF(
      List(
        (8, "bat"),
        (64, "mouse"),
        (-27, "horse")
      ), List(
        ("number", IntegerType, true),
        ("word", StringType, true)
      )
    )

    /** Creating Data Frame from an RDD */
    //Seq of columns
    val columns = Seq("language", "users_count")
    //Data in Seq collection
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    //Use parallelize to create rdd from data
    val rdd = sparkSession.sparkContext.parallelize(data)
    //Method 1 calling toDF() on rdd passing column names directly
    val dfFromRDD1 = rdd.toDF("language", "users_count")
    //Method 2 calling toDF() on rdd passing column name via Seq collection
    val dfFromRDD2 = sparkSession.createDataFrame(rdd).toDF(columns: _*)

    /** From CSV */
    val df2 = sparkSession.read
      .csv(s"file:///$inputPath/src/Input/questions_10K.csv")
    df2.show(10)

    /** From Hive
     * More on Hive in when we cover ETL
     * */
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkSession.sparkContext)
    hiveContext.sql("create table if not exists allperson(first_name string, last_name string, age int) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile ")
    //Loading data from a file
    hiveContext.sql(s"load data local inpath '$inputPath/src/Input/persons' into table allperson")
    val hiveDF = hiveContext.sql("select * from allperson")
    hiveDF.show()

    /** From JSON
     * */
    val df = sparkSession.read.json(s"file:///$inputPath/src/Input/people.json")
    df.show()

    /** From SparkSQL
     * More on SparkSQL in next session when we cover statistical analytics */
    df.createOrReplaceTempView("peopleTable")
    val dfSql = sparkSession.sqlContext.sql("SELECT * FROM peopleTable")
    dfSql.show()

    /** From RDBMS
     * Working example when we cover ETL in next session */
    //    val df_mysql = sparkSession.read.format("jdbc")
    //      .option("url", "jdbc:mysql://localhost/db")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .option("dbtable", "tablename")
    //      .option("user", "user")
    //      .option("password", "password")
    //      .load()

  }


  def DFOperations(): Unit = {

    val inputPath = s"${System.getProperty("user.dir")}".replace("\\", "/") //Current working directory
    /** Reading data frame from CSV with schema inference */
    val dfTags = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"file:///$inputPath/src/Input/question_tags_10K.csv")
      .toDF("id", "tag")
    //See data
    dfTags.show(10)
    //See schema of the data frame
    dfTags.printSchema()
    //See only few columns
    dfTags.select("id", "tag").show(10)
    //Filter data frame rows
    dfTags.filter("tag == 'php'").show(10)
    //Count with filter
    println(s"Number of php tags = ${dfTags.filter("tag == 'php'").count()}")
    //Usage of like operator
    dfTags.filter("tag like 's%'").show(10)
    //Combining filters
    dfTags
      .filter("tag like 's%'")
      .filter("id == 25 or id == 108")
      .show(10)
    //Usage of in operator
    dfTags.filter("id in (25, 108)").show(10)
    //Groupby operator with count aggregation
    dfTags.groupBy("tag").count().show(10)
    //Groupby with filter
    dfTags.groupBy("tag").count().filter("count > 5").show(10)
    //Groupby aggregation filter orderby together
    dfTags.groupBy("tag").count().filter("count > 5").orderBy("tag").show(10)

    //Reading using specific schema
    val dfQuestionsCSV = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(s"file:///$inputPath/src/Input/questions_10K.csv")
      .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")
    //See schema inferred
    dfQuestionsCSV.printSchema()
    //Adding user schema
    val dfQuestions = dfQuestionsCSV.select(
      dfQuestionsCSV.col("id").cast("integer"),
      dfQuestionsCSV.col("creation_date").cast("timestamp"),
      dfQuestionsCSV.col("closed_date").cast("timestamp"),
      dfQuestionsCSV.col("deletion_date").cast("date"),
      dfQuestionsCSV.col("score").cast("integer"),
      dfQuestionsCSV.col("owner_userid").cast("integer"),
      dfQuestionsCSV.col("answer_count").cast("integer")
    )
    //See user defined schema applied
    dfQuestions.printSchema()
    //See data in user defined schema
    dfQuestions.show(10)

    //Creating a subset data frame
    val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()

    dfQuestionsSubset.show()
    //Join two dataframes
    dfQuestionsSubset.join(dfTags, "id").show(10)
    //Join with select
    dfQuestionsSubset
      .join(dfTags, "id")
      .select("owner_userid", "tag", "creation_date", "score")
      .show(10)
    //Join specific columns
    dfQuestionsSubset
      .join(dfTags, dfTags("id") === dfQuestionsSubset("id"))
      .show(10)
    //Join type inner
    dfQuestionsSubset
      .join(dfTags, Seq("id"), "inner")
      .show(10)
    //join type left outer
    dfQuestionsSubset
      .join(dfTags, Seq("id"), "left_outer")
      .show(10)
    //join type right outer
    dfTags
      .join(dfQuestionsSubset, Seq("id"), "right_outer")
      .show(10)
    //Use of distinct. Here distinct column tag
    dfTags
      .select("tag")
      .distinct()
      .show(10)

    /**Sql statistical functions, below import is required*/
    //import org.apache.spark.sql.functions._
    //Average
    dfQuestions
      .select(avg("score"))
      .show()
    //Max
    dfQuestions
      .select(max("score"))
      .show()
    //Min
    dfQuestions
      .select(min("score"))
      .show()
    //Mean
    dfQuestions
      .select(mean("score"))
      .show()
    //Sum
    dfQuestions
      .select(sum("score"))
      .show()
    //Multiple aggregations with group by
    dfQuestions
      .filter("id > 400 and id < 450")
      .filter("owner_userid is not null")
      .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
      .groupBy(dfQuestions.col("owner_userid"))
      .agg(avg("score"), max("answer_count"))
      .show()
    //Describe the data frame generic statistical properties
    val dfQuestionsStatistics = dfQuestions.describe()
    dfQuestionsStatistics.show()

    //Correlation between two columns
    val correlation = dfQuestions.stat.corr("score", "answer_count")
    println(s"correlation between column score and answer_count = $correlation")
    //Co-variance between two columns
    val covariance = dfQuestions.stat.cov("score", "answer_count")
    println(s"covariance between column score and answer_count = $covariance")
    //Frequency
    val dfFrequentScore = dfQuestions.stat.freqItems(Seq("answer_count"))
    dfFrequentScore.show()
    //Cross tab - Rows to columns and columns to rows
    val dfScoreByUserid = dfQuestions
      .filter("owner_userid > 0 and owner_userid < 20")
      .stat
      .crosstab("score", "owner_userid")

    dfScoreByUserid.show(10)


    // Filter on aggregated values. Similar to groupby having in SQL.
    // find all rows where answer_count in (5, 10, 20)
    val dfQuestionsByAnswerCount = dfQuestions
      .filter("owner_userid > 0")
      .filter("answer_count in (5, 10, 20)")

    // count how many rows match answer_count in (5, 10, 20)
    dfQuestionsByAnswerCount
      .groupBy("answer_count")
      .count()
      .show()

    // Create a fraction map where we are only interested:
    // - 50% of the rows that have answer_count = 5
    // - 10% of the rows that have answer_count = 10
    // - 100% of the rows that have answer_count = 20
    // Note also that fractions should be in the range [0, 1]
    val fractionKeyMap = Map(5 -> 0.5, 10 -> 0.1, 20 -> 1.0)

    // Stratified sample using the fractionKeyMap.
    dfQuestionsByAnswerCount
      .stat
      .sampleBy("answer_count", fractionKeyMap, 7L)
      .groupBy("answer_count")
      .count()
      .show()

    // Note that changing the random seed will modify your sampling outcome.
    // Changing the random seed to 37.
    dfQuestionsByAnswerCount
      .stat
      .sampleBy("answer_count", fractionKeyMap, 37L)
      .groupBy("answer_count")
      .count()
      .show()

    // Approximate Quantile. Dividing frequency distribution to groups
    val quantiles = dfQuestions
      .stat
      .approxQuantile("score", Array(0, 0.5, 1), 0.25)
    println(s"Qauntiles segments = ${quantiles.toSeq}")

    // Bloom Filter - Element is definitely not in the set or may be in the set.
    val tagsBloomFilter = dfTags.stat.bloomFilter("tag", 1000L, 0.1)

    println(s"bloom filter contains java tag = ${tagsBloomFilter.mightContain("java")}")
    println(s"bloom filter contains some unknown tag = ${tagsBloomFilter.mightContain("unknown tag")}")

    // Count Min Sketch - Counting frequencies using hash. It may over count some frequencies due to collisions
    val cmsTag = dfTags.stat.countMinSketch("tag", 0.1, 0.9, 37)
    val estimatedFrequency = cmsTag.estimateCount("java")
    println(s"Estimated frequency for tag java = $estimatedFrequency")

    // Sampling With Replacement - Sampling where probability of one does not affect the probability outcome of another sample.
    val dfTagsSample = dfTags.sample(true, 0.2, 37L)
    println(s"Number of rows in sample dfTagsSample = ${dfTagsSample.count()}")
    println(s"Number of rows in dfTags = ${dfTags.count()}")


    //Getting a paired DF
    val seqTags = Seq(
      1 -> "java",
      1 -> "javascript",
      2 -> "erlang",
      3 -> "scala",
      3 -> "akka"
    )

    import sparkSession.implicits._
    val dfMoreTags = seqTags.toDF("id", "tag")
    dfMoreTags.show(10)

    //Use of union
    val dfUnionOfTags = dfTags
      .union(dfMoreTags)
      .filter("id in (1,3)")
    dfUnionOfTags.show(10)
    //use of intersection
    val dfIntersectionTags = dfMoreTags
      .intersect(dfUnionOfTags)
      .show(10)

    //Adding a column and aliasing
    //      import org.apache.spark.sql.functions._
    val dfSplitColumn = dfMoreTags
      .withColumn("tmp", split($"tag", "_"))
      .select(
        $"id",
        $"tag",
        $"tmp".getItem(0).as("so_prefix"),
        $"tmp".getItem(1).as("so_tag")
      ).drop("tmp")
    dfSplitColumn.show(10)

    //Specifying names
    val mangoes = Seq(("Alphonso", 1.50), ("Margo", 2.0), ("Fazli", 2.50))
    val dfMangoes = sparkSession.createDataFrame(mangoes)
      .toDF("Name", "Price")
    dfMangoes.show()
    val columnNames: Array[String] = dfMangoes.columns
    columnNames.foreach(name => println(s"$name"))

    val (colNames, colDataTypes) = dfMangoes.dtypes.unzip //Converts an array of column names and data types to separate arrays of names and types
    println(s"DataFrame column names = ${colNames.mkString(", ")}")
    println(s"DataFrame column data types = ${colDataTypes.mkString(", ")}")


    //Column exists
    val mangoes1 = Seq(("111", "Alphonso", 1.50), ("222", "Dasheri", 2.0), ("333", "Fazli", 2.50))

    val mangoesDF = sparkSession.createDataFrame(mangoes1)
      .toDF("Id", "Name", "Price")
    mangoesDF.show()

    val priceColumnExists = mangoesDF.columns.contains("Price")
    println(s"Does price column exist = $priceColumnExists")

    //Sub column data frame
    val inventory = Seq(("111", 10), ("222", 20), ("333", 30))
    val dfInventory = sparkSession
      .createDataFrame(inventory)
      .toDF("Id", "Inventory")
    dfInventory.show()

    //Joining the sub column back
    val dfMangoInventory = mangoesDF.join(dfInventory, Seq("Id"), "inner")
    dfMangoInventory.show()

  }
}
