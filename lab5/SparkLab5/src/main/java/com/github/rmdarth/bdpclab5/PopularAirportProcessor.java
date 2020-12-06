package com.github.rmdarth.bdpclab5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class PopularAirportProcessor {

    public JavaRDD<String[]> loadData(JavaSparkContext sc, String filename)
    {
        JavaRDD<String> origFile = sc.textFile(filename);
        return origFile.map(line -> line.split(","))
                       .filter(line -> !line[0].equals("YEAR"));
    }

    public JavaPairRDD<Integer, Tuple2<String,Integer>> getTopAirportsByMonth(JavaRDD<String[]> input)
    {
        return input
                // get key as "<month_airport>" and value 1 for each flight
                .mapToPair(elems -> new Tuple2<>(elems[1] + "_" + elems[8], 1))
                // reduce to count all flights by month_airport
                .reduceByKey( (f1, f2) -> f1 + f2 )
                // split the key, get all data mapped to month key only
                // value is { airport, count }
                .mapToPair(row -> {
                    String[] keys = row._1.split("_");
                    return new Tuple2<>(Integer.parseInt(keys[0]), new Tuple2<>(keys[1], row._2));
                })
                // get biggest { airport, count } for each month
                .reduceByKey((r1, r2) -> r1._2 > r2._2 ? r1 : r2)
                // sort by month num
                .sortByKey();
    }

    public void run(String flights, String output)
    {
        SparkConf conf = new SparkConf().setAppName("PopularAirportByMonth").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String[]> parsedCSV = loadData(sc, flights);

        JavaPairRDD<Integer, Tuple2<String,Integer>> maxInMonth =
                getTopAirportsByMonth(parsedCSV);

        System.out.println(maxInMonth.collect());

        // Store as TSV
        maxInMonth
                .map(vals -> vals._1.toString() + "\t" + vals._2.productIterator().mkString("\t"))
                .saveAsTextFile(output);
    }


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: popularairport <flight.csv> <outfolder>");
            System.exit(2);
        }

        new PopularAirportProcessor().run(remainingArgs[0], remainingArgs[1]);
    }
}
