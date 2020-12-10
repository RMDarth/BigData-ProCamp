package com.github.rmdarth.bdpclab5

import com.github.rmdarth.bdpclab5.CanceledFlights._
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, FunSuite}

class CanceledFlightsTest extends FunSuite with BeforeAndAfter {
  var sc : SparkContext = _

  before {
    sc = new SparkContext("local", "CanceledFlightsTest");

  }

  test("TestCanceledFlights"){
    val inputCSV =
      loadCSV(sc, getClass.getResource("/flights.csv").getPath(), "YEAR")
    val airlinesMap = getBroadcastedDimension(sc, getClass.getResource("/airlines.csv").getPath())

    val accums = getAccumulators(sc, airlinesMap.value)
    val processedData = process(inputCSV, accums)
    val resultList = processedData.sortBy(v => (v._1._1, -v._2)).collect()

    val expected: Array[((String, String), Float)] = Array(
      (("AA", "ANC"), 0.0f),
      (("AS", "ACT"), 1.0f),
      (("AS", "ANC"), 0.5f),
      (("AS", "LAX"), 0.25f),
      (("US", "ACT"), 0.0f),
      (("US", "LAX"), 0.0f),
    )
    assert(expected sameElements resultList)

    assert(accums("AA").value == 2)
    assert(accums("AS").value == 7)
    assert(accums("US").value == 3)
    assert(accums("UA").value == 0)
  }

  after {
    sc.stop();
  }

}
