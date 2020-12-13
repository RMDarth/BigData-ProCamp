package com.github.rmdarth.bdpclab6

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

// Accumulator containing 12 counters for each month
class MonthAccumulator extends AccumulatorV2[(Int, Long), mutable.Seq[Long]] {
  var monthData: mutable.Seq[Long] = mutable.Seq.fill(12)(0L)

  def this(newList: mutable.Seq[Long]) = {
    this()
    monthData = newList.clone()
  }

  override def isZero: Boolean = {
    for (i <- 0 until 12)
      if (monthData(i) != 0L) return false
    true
  }
  override def copy() = new MonthAccumulator(monthData)
  override def reset(): Unit = monthData = mutable.Seq.fill(12)(0L)
  override def add(data: (Int, Long)) = {
    monthData(data._1 - 1) = monthData(data._1 - 1) + data._2
  }
  override def merge(other: AccumulatorV2[(Int, Long), mutable.Seq[Long]]): Unit = {
    for (i <- 0 until 12)
      monthData(i) = monthData(i) + other.value(i)
  }
  override def value: mutable.Seq[Long] = monthData
}
