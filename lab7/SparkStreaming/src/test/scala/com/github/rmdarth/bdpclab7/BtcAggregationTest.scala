package com.github.rmdarth.bdpclab7

import com.github.rmdarth.bdpclab7.BtcAggregation.process
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.Matchers.{be, convertNumericToPlusOrMinusWrapper, convertToAnyShouldWrapper}
import org.scalatest.{BeforeAndAfter, FunSuite}

class BtcAggregationTest extends FunSuite with BeforeAndAfter {
  var spark : SparkSession = _

  before {
    Logger.getLogger("org").setLevel(Level.WARN)
    spark = SparkSession
      .builder
      .appName("BtcAggregationTestApp")
      .master("local")
      .getOrCreate()
  }

  test("TestBtcAggregation") {

    val input = spark.readStream.format("text")
      .load(getClass.getResource("/input").getPath)

    process(input)
      .writeStream
      .format("memory")
      .outputMode("complete")
      .queryName("result")
      .start()
      .processAllAvailable()

    val result = spark.sql("select Count, AvgPrice, SalesTotal from result")
      .collect()(0)

    assert(result.getLong(0) === 7)                // Count
    assert(result.getDouble(1) === 4.2 +- 0.001)   // Avg transaction price
    assert(result.getDouble(2) === 42.9 +- 0.001)  // Sales total
  }

  after {
    spark.stop();
  }
}