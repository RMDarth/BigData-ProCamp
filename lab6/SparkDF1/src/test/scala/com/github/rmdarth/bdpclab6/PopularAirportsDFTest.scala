package com.github.rmdarth.bdpclab6

import com.github.rmdarth.bdpclab6.PopularAirportsDF._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper}
import org.scalatest.{BeforeAndAfter, FunSuite}

class PopularAirportsDFTest extends FunSuite with BeforeAndAfter {
  var spark : SparkSession = _

  before {
    Logger.getLogger("org").setLevel(Level.WARN)
    spark = SparkSession
      .builder
      .appName("PopularAirportsDFTestApp")
      .master("local")
      .getOrCreate()
  }

  test("TestPopularAirports") {
    val (flights, airports) = loadData(
      spark,
      getClass.getResource("/flights.csv").getPath(),
      getClass.getResource("/airports.csv").getPath()
    )

    val accumsMap = createAccumulators(spark, airports.value)

    val resultList =
      process(flights, airports.value, accumsMap)
        .rdd.map(row => (row(0), row(1), row(2), row(3))).collect()

    val expected = Array(
      (1, "ABR", "Aberdeen Regional Airport", 3),
      (2, "A1", "Unknown", 3),
      (3, "FAR", "Hector International Airport", 2),
      (4, "ABR", "Aberdeen Regional Airport", 4)
    )

    resultList should be(expected)

    accumsMap("ABR").value should be(Seq[Long](3, 2, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0))
    accumsMap("FAR").value should be(Seq[Long](2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    accumsMap("ABE").value should be(Seq[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
  }

  after {
    spark.stop();
  }
}
