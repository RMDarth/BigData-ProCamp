package com.github.rmdarth.bdpclab5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class PopularAirportProcessor {

    public JavaRDD<String[]> loadData(JavaSparkContext sc, String filename)
    {
        JavaRDD<String> origFile = sc.textFile(filename);
        return origFile.map(line -> line.split(","))
                       .filter(line -> !line[0].equals("YEAR"));
    }

    public JavaPairRDD<String, String> loadAirports(JavaSparkContext sc, String filename)
    {
        JavaRDD<String> origFile = sc.textFile(filename);
        return origFile
                .mapToPair(line -> {
                    String[] vals = line.split(",");
                    return new Tuple2<>(vals[0], vals[1]);
                })
                .filter(line -> !line._1().equals("IATA_CODE"));
    }

    public JavaPairRDD<Integer, Tuple3<String,String,Integer>> getTopAirportsByMonth(
            JavaRDD<String[]> input,
            Broadcast<Map<String, String>> airportsData,
            Map<String, MonthAccumulator> airportsAccums)
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
                    int month = Integer.parseInt(keys[0]);
                    if (airportsAccums != null && airportsAccums.containsKey(keys[1]))
                        airportsAccums.get(keys[1]).add(new Tuple2<>(month, row._2.longValue()));
                    return new Tuple2<>(
                            month,
                            new Tuple3<>(
                                    keys[1], // Airport code
                                    airportsData.value().getOrDefault(keys[1], "Unknown"),  // Airport name
                                    row._2)); // Count
                })
                // get biggest { airport, count } for each month
                .reduceByKey((r1, r2) -> r1._3() > r2._3() ? r1 : r2)
                // sort by month num
                .sortByKey();
    }

    public void run(String flights, String airports, String output)
    {
        SparkConf conf = new SparkConf().setAppName("PopularAirportByMonth").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load data
        JavaRDD<String[]> parsedCSV = loadData(sc, flights);
        Broadcast<Map<String, String>> airportsData
                = sc.broadcast(loadAirports(sc, airports).collectAsMap());

        // create accumulators
        Map<String, MonthAccumulator> airportsAccums = new TreeMap<>();
        for (String key : airportsData.value().keySet()) {
            MonthAccumulator monthAccumulator = new MonthAccumulator();
            sc.sc().register(monthAccumulator, key);
            airportsAccums.put(key, monthAccumulator);
        }

        // process
        JavaPairRDD<Integer, Tuple3<String,String,Integer>> maxInMonth =
                getTopAirportsByMonth(parsedCSV, airportsData, airportsAccums);

        // Store as TSV
        maxInMonth
                .map(vals -> vals._1.toString() + "\t" + vals._2.productIterator().mkString("\t"))
                .saveAsTextFile(output);

        for (String airport : airportsAccums.keySet()) {
            System.out.println(airport + ": " + airportsAccums.get(airport).value());
        }

        sc.close();
    }


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 3) {
            System.err.println("Usage: popularairport <flight.csv> <airports.csv> <outfolder>");
            System.exit(2);
        }

        new PopularAirportProcessor().run(remainingArgs[0], remainingArgs[1], remainingArgs[2]);
    }
}
