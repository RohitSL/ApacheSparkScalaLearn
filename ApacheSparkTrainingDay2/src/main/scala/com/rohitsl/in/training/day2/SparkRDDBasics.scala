package com.rohitsl.in.training.day2

import com.rohitsl.in.training.day2.SparkRDDMain.sparkSession
import org.apache.spark.storage.StorageLevel
class SparkRDDBasics {
 /** Various methods of creating RDDs */
  def RDDCreate():Unit={
    //System.getProperty("user.dir") is current working directory
    val inputPath = s"${System.getProperty("user.dir")}".replace("\\","/")
    /** Using makeRDD call and passing a collection */
    val oneTo20 = 1 to 20
    val oneTo20RDD = sparkSession.sparkContext.makeRDD(oneTo20)
    oneTo20RDD.collect()

    /**Parallelizing a collection to get RDD*/
    val subjects=sparkSession.sparkContext.parallelize(Seq(("maths",52),("english",75),("science",82), ("computer",65),("maths",85)))
    val sorted = subjects.sortByKey()
    sorted.collect().foreach(println)

    val months = sparkSession.sparkContext.parallelize(Array("jan","feb","mar","april","may","jun"),3)
    val result = months.coalesce(2)
    result.collect().foreach(println)

    val numbers = Array(1, 2, 3, 4, 5)
    val numbersRDD = sparkSession.sparkContext.parallelize(numbers)

    val numbersRDD2 = sparkSession.sparkContext.parallelize(numbers,4)

    val sequenceRDD = sparkSession.sparkContext.parallelize(0 to 4, numSlices = 2).groupBy(_ % 2)

    val sequenceRDD2 = sparkSession.sparkContext.parallelize(0 to 8).groupBy(_ % 3)

    /**Reading from a CSV file to get RDD*/
    val csvRDD = sparkSession.read.csv(s"file:///$inputPath/src/Input/questions_10K.csv").rdd

    /**Reading from a JSON file to get RDD*/
    val jsonRDD = sparkSession.read.json(s"file:///$inputPath/src/Input/sampleJSON.json").rdd

    /**Reading from a text file to get RDD*/
    val txtRDD = sparkSession.read.textFile(s"file:///$inputPath/src/Input/justwords").rdd

    /**Creating from an existing RDD*/
    val words=sparkSession.sparkContext.parallelize(Seq("the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"))
    val wordPairRDD = words.map(w => (w.charAt(0), w))
    System.out.println(wordPairRDD.first)

    /**Creating RDD from a text file hdfs://, s3a://, file://, wasb:// */
    val lines = sparkSession.sparkContext.textFile(s"file:///$inputPath/src/Input/justwords",8)
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    //lineLengths.persist()
    //lineLengths.cache()


    /**PairedRDDFunctions where RDD has tuple data*/
    val lines1 = sparkSession.sparkContext.textFile(s"file:///$inputPath/src/Input/justwords")
    val pairs = lines1.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)

  }

  def RDDTransform():Unit={
    /**TRANSFORMATIONS*/
    val inputPath = s"${System.getProperty("user.dir")}".replace("\\","/")
    /**map(func):Return a new distributed dataset formed by passing each element of the source through
    a function func.*/
    val mapData = sparkSession.read.textFile(s"file:///$inputPath/src/Input/justwords").rdd
    val mapFile = mapData.map(line => (line,line.length))
    mapFile.foreach(println)

    /**flatMap(func):Similar to map, but each input item can be mapped to 0 or more output items (so func
    should return a Seq rather than a single item).*/
    val flatMapData = sparkSession.read.textFile(s"file:///$inputPath/src/Input/justwords").rdd
    val flatmapFile = flatMapData.flatMap(lines => lines.split(" "))
    flatmapFile.foreach(println)

    /**filter(func):Return a new dataset formed by selecting those elements of the source on which func
    returns true.*/
    val filterData = sparkSession.read.textFile(s"file:///$inputPath/src/Input/justwords").rdd
    val filterFile = filterData.flatMap(lines => lines.split(" ")).filter(value => value=="spark")
    println(filterFile.count())

    /**union(otherDataset): Return a new dataset that contains the union of the elements in the source dataset and the
    argument.*/
    val rddUnion1 = sparkSession.sparkContext.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014)))
    val rddUnion2 = sparkSession.sparkContext.parallelize(Seq((5,"dec",2014),(17,"sep",2015)))
    val rddUnion3 = sparkSession.sparkContext.parallelize(Seq((6,"dec",2011),(16,"may",2015)))
    val rddUnion = rddUnion1.union(rddUnion2).union(rddUnion3)
    rddUnion.foreach(println)

    /**intersection(otherDataset):Return a new RDD that contains the intersection of elements in the source
    dataset and the argument.*/
    val rddInter1 = sparkSession.sparkContext.parallelize(Seq((1,"jan",2016),(3,"nov",2014), (16,"feb",2014)))
    val rddInter2 = sparkSession.sparkContext.parallelize(Seq((5,"dec",2014),(1,"jan",2016)))
    val comman = rddInter1.intersection(rddInter2)
    comman.foreach(println)

    /**distinct([numPartitions])):Return a new dataset that contains the distinct elements of the source dataset.*/
    val rddDistinct1 = sparkSession.sparkContext.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014),(3,"nov",2014)))
    val resultDistinct = rddDistinct1.distinct()
    println(resultDistinct.collect().mkString(", "))

    /**groupByKey([numPartitions]):When called on a dataset of (K, V) pairs,returns a dataset of (K, Iterable<V>) pairs.
    Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using
    reduceByKey or aggregateByKey will yield much better performance.
    Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD.
    You can pass an optional numPartitions argument to set a different number of tasks.*/
    val dataGroupByKey = sparkSession.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val group = dataGroupByKey.groupByKey().collect()
    group.foreach(println)

    /**reduceByKey(func, [numPartitions]):When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs
     * where the values for each key are aggregated using the given reduce function func, which must be of type
     * (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.*/
    val words = Array("one","two","two","four","five","six","six","eight","nine","ten")
    val dataWords = sparkSession.sparkContext.parallelize(words).map(w => (w,1)).reduceByKey(_+_)
    dataWords.foreach(println)

    /**aggregateByKey(zeroValue)(seqOp, combOp,[numPartitions]):When called on a dataset of (K, V) pairs, returns a
      dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and
      a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while
      avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an
      optional second argument.*/
    val babyNames = sparkSession.sparkContext.parallelize(List(("David", 6), ("Abby", 4), ("David", 5), ("Abby", 5)))
    babyNames.aggregateByKey(0)((k,v) => v.toInt+k, (v,k) => k+v).collect.foreach(println)

    /**sortByKey([ascending], [numPartitions]):When called on a dataset of (K, V) pairs where K implements Ordered,
     * returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean
     * ascending argument.*/
    val dataSortByKey = sparkSession.sparkContext.parallelize(Seq(("maths",52), ("english",75), ("science",82), ("computer",65), ("maths",85)))
    val sorted = dataSortByKey.sortByKey()
    sorted.collect().foreach(println)

    /**join(otherDataset, [numPartitions]):When called on datasets of type (K, V) and (K, W), returns a dataset of
     * (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin,
     * rightOuterJoin, and fullOuterJoin.*/
    val dataJoin = sparkSession.sparkContext.parallelize(Array(('A',1),('b',2),('c',3)))
    val dataJoin2 =sparkSession.sparkContext.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val resultJoin = dataJoin.join(dataJoin2)
    println(resultJoin.collect().mkString(","))

    /**coalesce(numPartitions):Decrease the number of partitions in the RDD to numPartitions. Useful for running
     * operations more efficiently after filtering down a large dataset.*/
    val rddCoalesce1 = sparkSession.sparkContext.parallelize(Array("jan","feb","mar","april","may","jun"),3)
    System.out.println(s"Before coalesce number of partitions = ${rddCoalesce1.getNumPartitions}")
    val resultCoalesce = rddCoalesce1.coalesce(2)
    System.out.println(s"After coalesce number of partitions = ${resultCoalesce.getNumPartitions}")
    /**repartition(numPartitions):Reshuffle (move data around in partitions) the data in the RDD randomly to create
     * either more or fewer partitions and balance it across them. This always shuffles all data over the network.*/
    val resultRepartition = resultCoalesce.repartition(3)
    System.out.println(s"After repartition number of partitions = ${resultRepartition.getNumPartitions}")


    /**cogroup(otherDataset, [numPartitions]):When called on datasets of type (K, V) and (K, W), returns a dataset
    of (K, (Iterable<V>,Iterable<W>)) tuples. This operation is also called groupWith. Multiple Pair RDDs can be combined using cogroup*/
    val cogroupRDD1 = sparkSession.sparkContext.parallelize(Seq(("key1", 1),("key2", 2),("key1", 3)))
    val cogroupRDD2 = sparkSession.sparkContext.parallelize(Seq(("key1", 5),("key2", 4)))
    val cogrouped = cogroupRDD1.cogroup(cogroupRDD2)
    cogrouped.collect().foreach(println)

    /**cartesian(otherDataset):When called on datasets of types T and U,returns a dataset of (T, U) pairs
      (all pairs of elements).*/
    val cartesianRDD = sparkSession.sparkContext.parallelize(1 to 5)
    val combinations = cartesianRDD.cartesian(cartesianRDD).filter{ case (a,b) => a < b }
    combinations.collect().foreach(println)

    /**pipe(command, [envVars]):Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script.
    RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.*/
    val nums = sparkSession.sparkContext.makeRDD(Array("Hello","How", "Are","You"))
    nums.pipe(s"$inputPath/src/Input/myecho.cmd").collect().foreach(println)

    /**repartitionAndSortWithinPartitions(partitioner):Repartition the RDD according to the given
     * partitioner and, within each resulting partition, sort records by their keys. This is
     * more efficient than calling repartition and then sorting within each partition because it
     * can push the sorting down into the shuffle machinery.
     */
    val pairs  = sparkSession.sparkContext.parallelize(Array(("a",1), ("c",3),("b",2), ("d",3)))
    pairs.collect().foreach(println)
    pairs.repartitionAndSortWithinPartitions(new org.apache.spark.RangePartitioner(2,pairs)).collect().foreach(println)


     /**mapPartitions(func):Similar to map, but runs separately on each partition (block) of the RDD, so
     * func must be of type Iterator<T> => Iterator<U> when running on an RDD of type T.
     */
     val studentRDD =  sparkSession.sparkContext.parallelize(List("Arun","Radha","Gopal","Sudha","Hari"),3)
      val mappedStudents =   studentRDD.mapPartitions{
      //'iterator' to iterate through all elements in the partition
        (iterator) => {
          val myList = iterator.toList
          myList.map(x => x + " is present ").iterator
        }
      }
    mappedStudents.collect().foreach(println)


    /**mapPartitionsWithIndex(func):Similar to mapPartitions, but also provides func with an integer value
     * representing the index of the partition, so func must be of type (Int, Iterator<T>) => Iterator<U> when
     * running on an RDD of type T.
     */
    val colorRDD =  sparkSession.sparkContext.parallelize(List("yellow","red","blue","cyan","black"),3)
    val mappedColors =   colorRDD.mapPartitionsWithIndex{
                         // 'index' represents the Partition No
                         // 'iterator' to iterate through all elements
                         //                         in the partition
                         (index, iterator) => {
                              println("Called in Partition -> " + index)
                              val myList = iterator.toList
                              myList.map(x => x + " -> " + index).iterator
                           }
                      }
    mappedColors.collect().foreach(println)

    /**sample(withReplacement, fraction, seed):Sample a fraction fraction of the data, with or without replacement,
     * using a given random number generator seed.
     */
    val numSampleRDD = sparkSession.sparkContext.parallelize(1 to 10000, 3)
    numSampleRDD.sample(false, 0.1).collect().take(5).foreach(println)
    //fraction is probability of chosing an item in the RDD as sample.

    /**takeSample(withReplacement, num, [seed]):Return an array with a random sample of num elements of
     * the dataset, with or without replacement, optionally prespecifying a random number generator seed.
     */
    val numSampleRDD1 = sparkSession.sparkContext.parallelize(1 to 10000, 3)
    numSampleRDD1.takeSample(false,100,100000000).foreach(println)
  }

  def RDDAction():Unit={
    /**ACTIONS*/
    val inputPath = s"${System.getProperty("user.dir")}".replace("\\","/")
    /**top:If ordering is present in our RDD, then we can extract top elements from our RDD using top().
     Action top() use default ordering of data.*/
    val dataTop = sparkSession.read.textFile(s"file:///$inputPath/src/Input/justwords").rdd
    val mapTopFile = dataTop.map(line => (line,line.length))
    val resTop = mapTopFile.top(3)
    resTop.foreach(println)

    /**reduce(func):Aggregate the elements of the dataset using a function func (which takes two arguments and returns
     * one). The function should be commutative and associative so that it can be computed correctly in parallel.*/
    val rddReduce = sparkSession.sparkContext.parallelize(List(20,32,45,62,8,5))
    val sumReduce = rddReduce.reduce(_+_)
    println(sumReduce)

    /**fold:The signature of the fold() is like reduce(). Besides, it takes “zero value” as input, which is used
     * for the initial call on each partition. But, the condition with zero value is that it should be the identity
     * element of that operation. The key difference between fold() and reduce() is that, reduce() throws an exception
     * for empty collection, but fold() is defined for empty collection.*/
    val rddMarks = sparkSession.sparkContext.parallelize(List(("maths", 80),("science", 90)))
    val additionalMarks = ("extra", 4)
    val sumMarks = rddMarks.fold(additionalMarks){
      (acc, marks) => val add = acc._2 + marks._2
      ("total", add)
    }
    println(sumMarks)

    /**collect():Return all the elements of the dataset as an array at the driver program. This is usually
     * useful after a filter or other operation that returns a sufficiently small subset of the data.*/
    val dataCollect = sparkSession.sparkContext.parallelize(Array(('A',1),('b',2),('c',3)))
    val dataCollect2 =sparkSession.sparkContext.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val resultCollect = dataCollect.join(dataCollect2)
    println(resultCollect.collect().mkString(","))

    /**count():Return the number of elements in the dataset.*/
    val dataCount = sparkSession.read.textFile(s"file:///$inputPath/src/Input/justwords").rdd
    val mapFileCount = dataCount.flatMap(lines => lines.split(" ")).filter(value => value=="spark")
    println(mapFileCount.count())

    /**countByValue*/
    val dataCountByValue = sparkSession.read.textFile(s"file:///$inputPath/src/Input/justwords").rdd
    val resultCountByValue= dataCountByValue.map(line => (line,line.length)).countByValue()
    resultCountByValue.foreach(println)
    /**countByKey():Only available on RDDs of type (K, V). Returns a hashmap of
    *(K, Int) pairs with the count of each key.*/

    /**first():Return the first element of the dataset (similar to take(1)).*/
    val dataFirst = sparkSession.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val groupFirst = dataFirst.groupByKey()
    val firstRec = groupFirst.first()
    System.out.println(firstRec)

    /**take(n):Return an array with the first n elements of the dataset.*/
    val dataTake = sparkSession.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val groupTake = dataTake.groupByKey().collect()
    val twoRec = groupTake.take(2)
    twoRec.foreach(println)

    /**foreach(func):Run a function func on each element of the dataset. This is usually done for side effects
     * such as updating an Accumulator or interacting with external storage systems.  Note: modifying variables
     * other than Accumulators outside of the foreach() may result in undefined behavior. */
    val dataForEach = sparkSession.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val groupForEach = dataForEach.groupByKey().collect()
    groupForEach.foreach(println)


    //takeOrdered(n, [ordering]):Return the first n elements of the RDD using either their
    //natural order or a custom comparator.
    val a = sparkSession.sparkContext.makeRDD(Array(1,5,3,4,8,6,2,0))
    a.takeOrdered(5).foreach(println)

    //saveAsTextFile(path):Write the elements of the dataset as a text file (or set of text
    //files) in a given directory in the local filesystem, HDFS or any
    //other Hadoop-supported file system. Spark will call toString
    //on each element to convert it to a line of text in the file.
    val saveAsRDD = sparkSession.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val outPath = s"file:///$inputPath/src/Output/saveAsTextFileOut";
    //Delete if out file exists
    import org.apache.hadoop.fs.{FileSystem, Path}
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(outPath)))
      fs.delete(new Path(outPath), true)

    saveAsRDD.saveAsTextFile(outPath)
  }

  def RDDPersistence():Unit={
    //***Persistence persist() cache()
    val persistRDD = sparkSession.sparkContext.makeRDD(Seq(1,3,4,6,7,8,9,4,5))

    //DISK_ONLY: Store RDD as desearlized java objects on the disk
    persistRDD.persist(StorageLevel.DISK_ONLY) // persistRDD.persist(StorageLevel.DISK_ONLY_2) same as disk but replicate in two cluster nodes

    //RDD.unpersist():Spark automatically monitors cache usage on each node and drops out old data partitions in a
    //least-recently-used (LRU) fashion. If you would like to manually remove an RDD instead of waiting
    //for it to fall out of the cache, use the RDD.unpersist() method.
    persistRDD.unpersist()

    //MEMORY_AND_DISK:Store RDD as deserialized Java objects in the JVM. If the RDD
    //does not fit in memory, store the partitions that don't fit on disk,
    //and read them from there when they're needed.
    persistRDD.persist(StorageLevel.MEMORY_AND_DISK) //Similar to above persistRDD.persist(StorageLevel.MEMORY_AND_DISK_2)
    persistRDD.unpersist()

    //MEMORY_AND_DISK_SER  (Java and Scala):Similar to MEMORY_ONLY_SER, but spill partitions that don't fit
    //in memory to disk instead of recomputing them on the fly each
    //time they're needed.
    persistRDD.persist(StorageLevel.MEMORY_AND_DISK_SER) //Similar to above persistRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    persistRDD.unpersist()

    //MEMORY_ONLY:Store RDD as deserialized Java objects in the JVM. If the RDD
    //does not fit in memory, some partitions will not be cached and will
    //be recomputed on the fly each time they're needed. This is the
    //default level.
    persistRDD.persist(StorageLevel.MEMORY_ONLY) //Similar to above persistRDD.persist(StorageLevel.MEMORY_ONLY_2)
    persistRDD.unpersist()

    persistRDD.persist()
    persistRDD.unpersist()

    persistRDD.cache()
    persistRDD.unpersist()

    //OFF_HEAP (experimental): Similar to MEMORY_ONLY_SER, but store the data in off-heap
    //memory. This requires off-heap memory to be enabled.
    persistRDD.persist(StorageLevel.OFF_HEAP)
    persistRDD.unpersist()

    //MEMORY_ONLY_SER (Java and Scala):Store RDD as serialized Java objects (one byte array per
    //partition). This is generally more space-efficient than deserialized
    //objects, especially when using a fast serializer, but more CPUintensive
    //to read.
    persistRDD.persist(StorageLevel.MEMORY_ONLY_SER) //Similar to above persistRDD.persist(StorageLevel.MEMORY_ONLY_SER_2) store in two cluster nodes
    persistRDD.unpersist()

  }

  def RDDSpecials():Unit={
    //***Shared variables
    //Broadcast variables:Broadcast variables allow the programmer to keep a read-only variable cached on each machine
    //rather than shipping a copy of it with tasks. They can be used, for example, to give every node a
    //copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast
    //variables using efficient broadcast algorithms to reduce communication cost.
    val broadcastVar = sparkSession.sparkContext.broadcast(Array(1, 2, 3))
    broadcastVar.value

    //Accumulators:Accumulators are variables that are only “added” to through an associative and commutative
    //operation and can therefore be efficiently supported in parallel. They can be used to implement
    //counters (as in MapReduce) or sums. Spark natively supports accumulators of numeric types, and
    //programmers can add support for new types.
    val accum = sparkSession.sparkContext.longAccumulator("My Accumulator")
    sparkSession.sparkContext.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
    accum.value
  }
}
