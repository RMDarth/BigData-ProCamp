package com.github.rmdarth.bdpclab5;

import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

public class MonthAccumulator extends AccumulatorV2<Tuple2<Integer, Long>, List<Long>>  {
    private List<Long> monthData;

    MonthAccumulator() {
        monthData = new ArrayList<>(Collections.nCopies(12, 0L));
    }

    MonthAccumulator(List<Long> data) {
        monthData = data;
    }

    @Override
    public boolean isZero() {
        return false;
    }

    @Override
    public AccumulatorV2 copy() {
        return new MonthAccumulator(monthData);
    }

    @Override
    public void reset() {
        for (int i = 0; i < 12; i++)
            monthData.set(i, 0L);
    }

    @Override
    public void add(Tuple2<Integer, Long> v) {
        monthData.set(v._1, monthData.get(v._1) + v._2);
    }

    @Override
    public void merge(AccumulatorV2<Tuple2<Integer, Long>, List<Long>> other) {
        for (int i = 0; i < 12; i++)
            monthData.set(i, monthData.get(i) + other.value().get(i));
    }

    @Override
    public List<Long> value() {
        return monthData;
    }
}