package com.github.rmdarth.bdpclab6

import com.github.rmdarth.bdpclab6.CanceledFlightsDF._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper}
import org.scalatest.{BeforeAndAfter, FunSuite}

class CanceledFlightsDFTest extends FunSuite with BeforeAndAfter {
  var spark : SparkSession = _

  before {
    Logger.getLogger("org").setLevel(Level.WARN)
    spark = SparkSession
      .builder
      .appName("CanceledFlightsDFTestApp")
      .master("local")
      .getOrCreate()
  }

  test("TestCanceledFlights") {
    val (flights, airports, airlines) = loadData(
      spark,
      getClass.getResource("/flights.csv").getPath,
      getClass.getResource("/airports.csv").getPath,
      getClass.getResource("/airlines.csv").getPath
    )

    val accumMap = createAccumulators(spark, airlines.value)

    val calculation = process(flights, accumMap)
    val resultList = addDimensions(calculation, airports.value, airlines.value).collect()

    val expected: Array[Row] = Array(
      Row("AA", "American Airlines Inc.",  "ANC", "Ted Stevens Anchorage International Airport",  0.0f),
      Row("AS", "Alaska Airlines Inc.",    "ACT", "Waco Regional Airport",                        1.0f),
      Row("AS", "Alaska Airlines Inc.",    "ANC", "Ted Stevens Anchorage International Airport",  0.5f),
      Row("AS", "Alaska Airlines Inc.",    "LAX", "Los Angeles International Airport",            0.25f),
      Row("US", "US Airways Inc.",         "ACT", "Waco Regional Airport",                        0.0f),
      Row("US", "US Airways Inc.",         "LAX", "Los Angeles International Airport",            0.0f),
    )

    resultList should be(expected)

    assert(accumMap("AA").value === 2)
    assert(accumMap("AS").value === 7)
    assert(accumMap("US").value === 3)
    assert(accumMap("UA").value === 0)
  }

  after {
    spark.stop();
  }
}
