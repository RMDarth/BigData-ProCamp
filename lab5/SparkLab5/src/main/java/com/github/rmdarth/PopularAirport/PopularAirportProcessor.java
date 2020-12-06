package com.github.rmdarth.PopularAirport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PopularAirportProcessor {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PopularAirportByMonth").setMaster("yarn-cluster");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> origFile = sc.textFile("/bdpc/hadoop_mr/airline/input/flights.csv");
        JavaRDD<String[]> parsedCSV = origFile.map(line -> line.split(","));
        JavaRDD<String[]> removedHeader = parsedCSV.filter(line -> !line[0].equals("YEAR"));
        JavaPairRDD<String, Integer> monthAirportKey =
                removedHeader.mapToPair(elems -> new Tuple2<>(elems[1] + "_" + elems[8], 1));
        JavaPairRDD<String, Integer> countedAirports =
                monthAirportKey.reduceByKey( (f1, f2) -> f1 + f2 ); // sum all flights
        JavaPairRDD<Integer, Tuple2<String,Integer>> groupByMonth =
                countedAirports.mapToPair(row -> {
                    String[] keys = row._1.split("_");
                    return new Tuple2<>(Integer.parseInt(keys[0]), new Tuple2<>(keys[1], row._2));
                });
        JavaPairRDD<Integer, Tuple2<String,Integer>> maxInMonth =
                groupByMonth.reduceByKey((r1, r2) -> r1._2 > r2._2 ? r1 : r2);

        System.out.println(maxInMonth.sortByKey().collect());
        maxInMonth.sortByKey().map(vals -> vals._1.toString() + "\t" + vals._2.productIterator().mkString("\t"))
                  .saveAsTextFile("/bdpc/hadoop_mr/airline/popularAirports");
    }
}
