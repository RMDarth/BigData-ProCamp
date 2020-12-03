package com.github.rmdarth.airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

public class AirlineDelayProcessor
{
    public static class AirlineMapper extends Mapper<Object, Text, Text, IntWritable> {
        enum CountersEnum {INPUT_FLIGHTS}

        private IntWritable delayTime = new IntWritable(0);
        private Text airlineCode = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] attributes = value.toString().split(",");

            try {
                airlineCode.set(attributes[4]);
                delayTime.set(Integer.parseInt(attributes[11]));
                context.write(airlineCode, delayTime);

                Counter counter = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.INPUT_FLIGHTS.toString());
                counter.increment(1);
            }
            catch (NumberFormatException e)
            {
                if (!attributes[0].equals("YEAR"))
                    e.printStackTrace();
            }
        }
    }

    public static class AvgDelayReducer extends Reducer<Text, IntWritable, Text, Text> {
        private TreeMap<Float, String> airlineAvgMap;
        private TreeMap<String, String> airlineNameMap;

        private Text result = new Text();
        private Text key = new Text();

        private Configuration conf;
        private BufferedReader reader;

        @Override
        protected void setup(Context context) throws IOException {
            airlineNameMap = new TreeMap<>();
            airlineAvgMap = new TreeMap<>();
            URI airlinesURI = Job.getInstance(context.getConfiguration()).getCacheFiles()[0];
            loadAirlineNames(new Path(airlinesURI.getPath()).getName());
        }

        private void loadAirlineNames(String fileName) {
            try {
                reader = new BufferedReader(new FileReader(fileName));
                String csvLine = null;
                while ((csvLine = reader.readLine()) != null) {
                    String[] attributes = csvLine.split(",");
                    airlineNameMap.put(attributes[0], attributes[1]);
                    System.out.println(csvLine);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count ++;
            }
            airlineAvgMap.put(sum / count, key.toString());
            if (airlineAvgMap.size() > 5)
            {
                airlineAvgMap.remove(airlineAvgMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Float, String> entry : airlineAvgMap.entrySet())
            {
                String airlineCode = entry.getValue().toString();
                result.set(airlineNameMap.get(airlineCode) + "\t" + entry.getKey().toString());
                key.set(entry.getValue());
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 3) {
            System.err.println("Usage: airlinedeparture <flight> <airlines> <out>");
            System.exit(2);
        }

        // Get average delay for all airlines
        Job jobAvgDelay = Job.getInstance(conf, "airline avg delay");
        jobAvgDelay.setJarByClass(AirlineDelayProcessor.class);
        jobAvgDelay.setMapperClass(AirlineMapper.class);
        jobAvgDelay.setReducerClass(AvgDelayReducer.class);
        jobAvgDelay.setOutputKeyClass(Text.class);
        jobAvgDelay.setOutputValueClass(Text.class);
        jobAvgDelay.setMapOutputKeyClass(Text.class);
        jobAvgDelay.setMapOutputValueClass(IntWritable.class);
        jobAvgDelay.addCacheFile(new Path(remainingArgs[1]).toUri());
        jobAvgDelay.setNumReduceTasks(1);

        FileInputFormat.addInputPath(jobAvgDelay, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(jobAvgDelay, new Path(remainingArgs[2]));

        System.exit(jobAvgDelay.waitForCompletion(true) ? 0 : 1);
    }
}
