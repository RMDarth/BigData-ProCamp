package com.github.rmdarth.bdpclab5
import java.io.BufferedOutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object CanceledFlights {

  def loadCSV(sc: SparkContext, filename: String, headerFirstElem: String): RDD[Array[String]] = {
    sc.textFile(filename)
      .map(line => line.split(","))
      .filter( _(0) != headerFirstElem)
  }

  def process(input: RDD[Array[String]]): RDD[((String, String), Float)] = {
    // airline is line[4], origin airport is line[7], cancel is line[24]
    input
      .map(line => (
        (line(4), line(7)),  // key (airline, airport)
        (if (line(24) == "0") 0f else 1f, 1f) // value (cancel, total)
      ))
      .reduceByKey((i1, i2) => (i1._1 + i2._1, i1._2 + i2._2))
      .map{ case((airline, airport), (cancel, total)) => ((airline, airport), cancel/total) }

  }

  def getBroadcastedDimension(sc: SparkContext, filename: String): Broadcast[Map[String, String]] = {
    sc.broadcast(
      loadCSV(sc, filename, "IATA_CODE")
        .map(line => (line(0), line(1)))
        .collect().toMap)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "CanceledFlights")
    val inputCSV =
      loadCSV(sc, "/Users/darth/Downloads/airlines/flights_small_cancel.csv", "YEAR")
    val airlinesMap = getBroadcastedDimension(sc, "/Users/darth/Downloads/airlines/airlines.csv")
    val airportsMap = getBroadcastedDimension(sc, "/Users/darth/Downloads/airlines/airports.csv")
    val processedData = process(inputCSV)
    val outputData = processedData
      .map( { case((airline, airport), cancelRate)
                => ((airlinesMap.value(airline), airline), (airportsMap.value(airport), airport, cancelRate)) } )
      .sortBy(v => (v._1, -v._2._3))

    /// Output JSON for airlines
    val json = "airlines" -> outputData
      .groupByKey()
      .filter( {case((name, code), data) => name != "American Airlines Inc." })
      .collect().toList.map{
        case (airline, nodes ) =>
            ("name" -> airline._1) ~
              ("code" -> airline._2) ~
              ("airports" -> nodes.map {
                a => ("name", a._1) ~
                  ("code", a._2) ~
                  ("cancelRate", a._3)
              })}

    val fs = FileSystem.get(sc.hadoopConfiguration);
    val output = fs.create(new Path("/Users/darth/Downloads/airlines/output.json"));
    val os = new BufferedOutputStream(output)
    os.write(compact(render(json)).getBytes("UTF-8"))
    os.close()

    // Output CSV for AA
    outputData.filter(_._1._1 == "American Airlines Inc." )
      .map( { case((airline, airlineCode), (airport, airportCode, cancelRate))
                => List(airline, airlineCode, airport, airportCode, cancelRate).mkString(",") } )
      .coalesce(1)
      .saveAsTextFile("/Users/darth/Downloads/airlines/output.csv")


    sc.stop()
  }
}
