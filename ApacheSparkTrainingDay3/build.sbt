name := "ApacheSparkTraining"

version := "0.1"

scalaVersion := "2.11.1"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "javax.activation" % "activation" % "1.1.1",
  "org.apache.spark" % "spark-core_2.11" % "2.3.3",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.3",
  "org.apache.spark" % "spark-mllib_2.11" % "2.3.3",
  "org.apache.spark" % "spark-hive_2.11" % "2.3.3",
  "mrpowers" % "spark-daria" % "0.26.1-s_2.11"
)
mainClass in (Compile, run) := Some("com.microsoft.training.in.day3")
mainClass in (Compile, packageBin) := Some("com.microsoft.training.in.day3")
