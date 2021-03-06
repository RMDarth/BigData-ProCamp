package com.github.rmdarth.bdpclab5
import java.io.BufferedOutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object CanceledFlights {

  def loadCSV(sc: SparkContext, filename: String, headerFirstElem: String): RDD[Array[String]] = {
    sc.textFile(filename)
      .map(line => line.split(","))
      .filter( _(0) != headerFirstElem)
  }

  def process(input: RDD[Array[String]], airlineAccums: Map[String, LongAccumulator]): RDD[((String, String), Float)] = {
    // airline is line[4], origin airport is line[7], cancel is line[24]
    input
      // grouping by (airline,airport) key
      .map(line => (
        (line(4), line(7)),  // key (airline, airport)
        (if (line(24) == "0") 0f else 1f, { airlineAccums(line(4)).add(1); 1 }) // value (cancel, total)
      ))
      // get total cancel and total flight
      .reduceByKey((i1, i2) => (i1._1 + i2._1, i1._2 + i2._2))
      // calculate percentage
      .map{ case((airline, airport), (cancel, total)) => ((airline, airport), cancel/total) }
  }

  def getBroadcastedDimension(sc: SparkContext, filename: String): Broadcast[Map[String, String]] = {
    sc.broadcast(
      loadCSV(sc, filename, "IATA_CODE")
        .map(line => (line(0), line(1)))
        .collect().toMap)
  }

  def getValueOrUnknown(map: Broadcast[Map[String,String]], key: String): String = {
    if (map.value.contains(key)) map.value(key) else "Unknown"
  }

  def getAccumulators(sc: SparkContext, airlines: Map[String, String]) = {
    var airlineAccumsMutable = collection.mutable.Map[String, LongAccumulator]()
    for (airline <- airlines.keys) {
      airlineAccumsMutable += (airline -> sc.longAccumulator(airline))
    }
    airlineAccumsMutable.toMap
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: canceledFlights <flight.csv> <airlines.csv> <airports.csv> <outfolder>")
      sys.exit(2)
    }

    val specialAirport = "Waco Regional Airport";
    val sc = new SparkContext("yarn", "CanceledFlights")

    // load data and dimensions
    val inputCSV =
      loadCSV(sc, args(0), "YEAR")
    val airlinesMap = getBroadcastedDimension(sc, args(1))
    val airportsMap = getBroadcastedDimension(sc, args(2))

    // create accumulators
    val airlineAccums = getAccumulators(sc, airlinesMap.value)

    // process data
    val processedData = process(inputCSV, airlineAccums)
    val outputData = processedData
      // add dimensions data
      .map( { case((airline, airport), cancelRate)
                => ((getValueOrUnknown(airlinesMap,airline), airline), (getValueOrUnknown(airportsMap, airport), airport, cancelRate)) } )
      // sort by airline and cancel rate
      .sortBy(v => (v._1, -v._2._3))

    // output JSON for airlines
    val json = "airlines" -> outputData
      .filter( {case(_, (airport, _, _)) => airport != specialAirport })
      .groupByKey()
      .collect().toList.map{
        case (airline, airports) =>
            ("name" -> airline._1) ~
              ("code" -> airline._2) ~
              ("airports" -> airports.map {
                case(name, code, cancelRate) => ("name", name) ~
                  ("code", code) ~
                  ("cancelRate", cancelRate)
              })}

    val fs = FileSystem.get(sc.hadoopConfiguration);
    val output = fs.create(new Path(args(3) + "/output.json"));
    val os = new BufferedOutputStream(output)
    os.write(compact(render(json)).getBytes("UTF-8"))
    os.close()

    // output CSV for Waco Regional Airport
    outputData.filter(_._2._1 == specialAirport )
      .map( { case((airline, airlineCode), (airport, airportCode, cancelRate))
                => List(airline, airlineCode, airport, airportCode, cancelRate).mkString(",") } )
      .coalesce(1)
      .saveAsTextFile(args(3) + "/output_csv")

    // print accumulators
    println("Total flights by airline: ")
    for ((airline, accum) <- airlineAccums) {
      println(airline + ": " + accum.sum)
    }

    sc.stop()
  }
}
