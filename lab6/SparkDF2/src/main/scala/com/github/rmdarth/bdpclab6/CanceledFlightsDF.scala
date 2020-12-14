package com.github.rmdarth.bdpclab6

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator

object CanceledFlightsDF {

  def loadData(spark: SparkSession,
               flightsFile: String,
               airportsFile: String,
               airlinesFile: String): (DataFrame, Broadcast[DataFrame], Broadcast[DataFrame]) = {
    val flights = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(flightsFile)

    val airports = spark.sparkContext.broadcast(
      spark.read
        .option("header", "true")
        .csv(airportsFile))

    val airlines = spark.sparkContext.broadcast(
      spark.read
        .option("header", "true")
        .csv(airlinesFile))

    (flights, airports, airlines)
  }

  // init accumulator for each airport
  def createAccumulators(spark: SparkSession, airlines: DataFrame): Map[String, LongAccumulator] = {
    var airlineAccumsMutable = collection.mutable.Map[String, LongAccumulator]()
    for (airline <- airlines.collect()) {
      airlineAccumsMutable +=
        (airline.getString(0) -> spark.sparkContext.longAccumulator(airline.getString(0)))
    }
    airlineAccumsMutable.toMap
  }

  def process(flights: DataFrame, accumMap: Map[String, LongAccumulator]): DataFrame = {
    val grouped = flights
      .groupBy("AIRLINE", "ORIGIN_AIRPORT")
      .agg(
        sum("CANCELLED").as("CANCELLED"),
        count("ORIGIN_AIRPORT").as("TOTAL"))
      .cache()

    // process accums
    grouped.foreach(row => accumMap(row.getString(0)).add(row.getLong(3)))

    grouped
      .withColumn("CANCEL_PERCENT", col("CANCELLED") / col("TOTAL"))
      .select("AIRLINE", "ORIGIN_AIRPORT", "CANCEL_PERCENT")
  }

  def addDimensions(cancelInfo: DataFrame,
                    airports: DataFrame,
                    airlines: DataFrame): DataFrame = {
    import cancelInfo.sparkSession.implicits._
    cancelInfo.as("C")
      .join(airports.as("P"), col("C.ORIGIN_AIRPORT") === col("P.IATA_CODE"), "left")
      .join(airlines.as("A"), col("C.AIRLINE") === col("A.IATA_CODE"), "left")
      .select(
        $"C.AIRLINE".as("AIRLINE_CODE"),
        $"A.AIRLINE".as("AIRLINE_NAME"),
        $"C.ORIGIN_AIRPORT".as("AIRPORT_CODE"),
        $"P.AIRPORT".as("AIRPORT_NAME"),
        $"C.CANCEL_PERCENT")
      .orderBy(asc("AIRLINE_CODE"), desc("CANCEL_PERCENT"))
      .na.fill("Unknown")
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: SparkDF2 <flight.csv> <airports.csv> <airlines.csv> <outfolder>")
      sys.exit(2)
    }

    // init spark
    val spark = SparkSession
      .builder
      .appName("BDPC-Lab6-Task2-CanceledFlights")
      .master("yarn")
      .getOrCreate()
    import spark.implicits._

    // load data and init accums
    val (flights, airports, airlines) = loadData(
      spark,
      args(0),
      args(1),
      args(2)
    )
    val accumMap = createAccumulators(spark, airlines.value)

    // process data and get canceled rate
    val calculation = process(flights, accumMap).cache()
    val result = addDimensions(calculation, airports.value, airlines.value).cache()

    // save to output files
    result
      .where($"AIRPORT_NAME" =!= "Waco Regional Airport")
      .coalesce(1)
      .write
      .json(args(3) + "/output_json")

    result
      .where($"AIRPORT_NAME" === "Waco Regional Airport")
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(args(3) + "/output_csv")

    // print accumulators
    println("Total flights by airline: ")
    for ((airline, accum) <- accumMap) {
      println(airline + ": " + accum.sum)
    }

    spark.close()
  }
}
