package com.github.rmdarth.bdpclab6

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object PopularAirportsDF {
  // load input csvs (and broadcast dimension)
  def loadData(spark: SparkSession, flightsFile: String, airportsFile: String): (DataFrame, Broadcast[DataFrame]) = {
    val flights = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(flightsFile)

    val airports = spark.sparkContext.broadcast(
      spark.read
        .option("header", "true")
        .csv(airportsFile))

    (flights, airports)
  }

  // init accumulator for each airport
  def createAccumulators(spark: SparkSession, airports: DataFrame): mutable.Map[String, MonthAccumulator] = {
    var airportsAccums = mutable.Map[String, MonthAccumulator]()
    airports.collect().foreach{ row =>
      val monthAccumulator = new MonthAccumulator()
      spark.sparkContext.register(monthAccumulator, row.getString(0))
      airportsAccums += (row.getString(0) -> monthAccumulator)
      ()
    }
    spark.sparkContext.broadcast(airportsAccums).value
  }

  // get most popular airports by month
  def process(flights: DataFrame,
              airports: DataFrame,
              accumsMap: mutable.Map[String, MonthAccumulator]): DataFrame = {
    // get count of flights for each airport by month
    val airportMonthCount = flights
      .groupBy("MONTH", "DESTINATION_AIRPORT").count()
      .cache()

    // update accums
    airportMonthCount.foreach(row => {
      if (accumsMap.contains(row.getString(1)))
        accumsMap(row.getString(1)).add((row.getInt(0), row.getLong(2)))
    })

    // get top airport for each month
    val topAirportMonth = airportMonthCount
      .groupBy("MONTH").agg(max("count").as("count"))

    topAirportMonth
      // add airport code to count
      .join(airportMonthCount, Seq("MONTH", "count"), "INNER")
      .withColumnRenamed("count", "FLIGHT_NUM")
      // join with airport names
      .join(airports, col("DESTINATION_AIRPORT") === col("IATA_CODE"), "left")
      .select("MONTH", "DESTINATION_AIRPORT", "AIRPORT", "FLIGHT_NUM")
      .orderBy("MONTH")
      .na.fill("Unknown")
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: SparkDF1 <flight.csv> <airports.csv> <outfolder>")
      sys.exit(2)
    }

    // init spark
    val spark = SparkSession
      .builder
      .appName("BDPC-Lab6-Task1-PopularAirports")
      .master("yarn")
      .getOrCreate()

    // load data and init accums
    val (flights, airports) = loadData(
      spark,
      args(0),
      args(1)
    )
    val accumsMap = createAccumulators(spark, airports.value)

    // process data and get most popular airports
    val topAirports = process(flights, airports.value, accumsMap)

    // output result
    topAirports.coalesce(1)
      .write.option("sep","\t").option("header","true")
      .csv(args(2))

    for (airport <- accumsMap.keySet) {
      println(airport + ": " + accumsMap(airport).value)
    }

    spark.close()
  }
}
