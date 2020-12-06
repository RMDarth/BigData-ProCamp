package com.github.rmdarth.bdpclab5;

import junit.framework.TestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.*;

import java.util.Arrays;
import java.util.List;

public class PopularAirportProcessorTest extends TestCase {
    PopularAirportProcessor airportProcessor;
    JavaSparkContext context;

    @Before
    public void setUp() {
        airportProcessor = new PopularAirportProcessor();

        SparkConf conf = new SparkConf().setAppName("TestApp").setMaster("local");
        context = new JavaSparkContext(conf);
    }

    @Test
    public void testFlights() throws Exception {
        JavaRDD<String[]> data = airportProcessor.loadData(
                context,
                getClass().getResource("/flights.csv").getPath());

        List<String[]> result = airportProcessor
                    .getTopAirportsByMonth(data)
                    .map(line -> new String[] {line._1.toString(), line._2._1, line._2._2.toString()})
                    .collect();

        String[][] expected = new String[][]{
                {"1", "A2", "3"},
                {"2", "A1", "2"},
                {"3", "A3", "2"},
                {"4", "A2", "4"}
        };

        Assert.assertArrayEquals(result.toArray(), expected);
    }
}