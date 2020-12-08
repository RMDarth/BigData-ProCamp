package com.github.rmdarth.bdpclab5;

import junit.framework.TestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.*;

import java.util.List;
import java.util.Map;

public class PopularAirportProcessorTest extends TestCase {
    PopularAirportProcessor airportProcessor;
    JavaSparkContext context;

    @Before
    public void setUp() {
        airportProcessor = new PopularAirportProcessor();

        SparkConf conf = new SparkConf().setAppName("TestApp").setMaster("local");
        context = new JavaSparkContext(conf);
    }

    @After
    public void finalize()
    {
        context.close();
    }

    @Test
    public void testFlights() throws Exception {
        JavaRDD<String[]> data = airportProcessor.loadData(
                context,
                getClass().getResource("/flights.csv").getPath());

        Broadcast<Map<String, String>> airports = context.broadcast(
                airportProcessor.loadAirports(
                    context,
                    getClass().getResource("/airports.csv").getPath()).collectAsMap());

        List<String[]> result = airportProcessor
                .getTopAirportsByMonth(data, airports, null)
                .map(line -> new String[]{line._1.toString(), line._2._1(), line._2._2(), line._2._3().toString()})
                .collect();

        String[][] expected = {
                {"1", "ABR", "Aberdeen Regional Airport", "3"},
                {"2", "A1", "Unknown", "2"},
                {"3", "FAR", "Hector International Airport", "2"},
                {"4", "ABR", "Aberdeen Regional Airport", "4"}
        };

        Assert.assertArrayEquals(result.toArray(), expected);
    }
}