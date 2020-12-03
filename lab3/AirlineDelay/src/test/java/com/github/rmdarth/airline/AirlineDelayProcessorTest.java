package com.github.rmdarth.airline;

import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.*;
import org.junit.*;

import java.io.*;
import java.net.URISyntaxException;

public class AirlineDelayProcessorTest extends TestCase {
    private MapDriver<Object, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, Text> reducerDriver;
    private MapReduceDriver<Object, Text, Text, IntWritable, Text, Text> mapReduceDriver;

    @Before
    public void setUp() throws URISyntaxException {
        AirlineDelayProcessor.AirlineMapper mapper = new AirlineDelayProcessor.AirlineMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        AirlineDelayProcessor.AvgDelayReducer reducer = new AirlineDelayProcessor.AvgDelayReducer();
        reducerDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.addCacheFile(getClass().getResource("/airlines.csv").toURI());
    }

    @Test
    public void testCorrectGenericFlights() throws Exception {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(
                    new InputStreamReader(getClass().getResource("/generalFlights.csv").openStream()));
            String csvLine = null;
            int lineNumber = 0;
            while ((csvLine = reader.readLine()) != null) {
                mapReduceDriver.withInput(new IntWritable(lineNumber), new Text(csvLine));
            }
        } finally {
            if (reader != null)
                reader.close();
        }

        mapReduceDriver.withOutput(new Text("AA"), new Text("American Airlines Inc.\t0.0"));
        mapReduceDriver.withOutput(new Text("AS"), new Text("Alaska Airlines Inc.\t6.0"));
        mapReduceDriver.withOutput(new Text("US"), new Text("US Airways Inc.\t1.0"));
        mapReduceDriver.runTest();
    }

    @Test
    public void testInvalidCSVValues() throws Exception {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(
                    new InputStreamReader(getClass().getResource("/incorrectFlights.csv").openStream()));
            String csvLine = null;
            int lineNumber = 0;
            while ((csvLine = reader.readLine()) != null) {
                mapReduceDriver.withInput(new IntWritable(lineNumber), new Text(csvLine));
            }
        } finally {
            if (reader != null)
                reader.close();
        }

        mapReduceDriver.withOutput(new Text("AS"), new Text("Alaska Airlines Inc.\t9.5"));
        mapReduceDriver.withOutput(new Text("XX"), new Text("null\t2.0"));
        mapReduceDriver.runTest();
    }
}