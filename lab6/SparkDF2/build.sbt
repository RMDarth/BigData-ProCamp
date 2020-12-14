name := "SparkDF2"

version := "1.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M3",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)