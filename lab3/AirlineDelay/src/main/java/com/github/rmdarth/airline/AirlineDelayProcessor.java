package com.github.rmdarth.airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class AirlineDelayProcessor
{
    public static class AirlineMapper extends Mapper<Object, Text, Text, IntWritable> {
        enum CountersEnum {INPUT_FLIGHTS}

        private IntWritable delayTime = new IntWritable(0);
        private Text airlineCode = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] attributes = value.toString().split(",");
            if (attributes.length < 12 || attributes[0].equals("YEAR") || attributes[11].isEmpty())
                return; // skip header and empty delays

            try {
                airlineCode.set(attributes[4]);
                delayTime.set(Integer.parseInt(attributes[11]));
                context.write(airlineCode, delayTime);
            } catch (NumberFormatException ex) {
                System.err.println("Can't parse delay time: " + ex.getMessage());
            }

            Counter counter = context.getCounter(CountersEnum.class.getName(),
                    CountersEnum.INPUT_FLIGHTS.toString());
            counter.increment(1);
        }
    }

    public static class AvgDelayReducer extends Reducer<Text, IntWritable, Text, Text> {
        private HashMap<String, String> airlineNameMap;
        private Text result = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            airlineNameMap = new HashMap<>();
            URI airlinesURI = Job.getInstance(context.getConfiguration()).getCacheFiles()[0];
            loadAirlineNames(context, airlinesURI);
        }

        private void loadAirlineNames(Context context, URI airlinesUri) throws IOException {
            BufferedReader reader = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(airlinesUri.toString());
                reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String csvLine = null;
                while ((csvLine = reader.readLine()) != null) {
                    String[] attributes = csvLine.split(",");
                    airlineNameMap.put(attributes[0], attributes[1]);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the airlines file '"
                        + StringUtils.stringifyException(ioe));
            } finally {
                if (reader != null)
                    reader.close();
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

            result.set(airlineNameMap.get(key.toString()) + "\t" + Float.toString(sum / count));
            context.write(key, result);
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

        Job jobAvgDelay = Job.getInstance(conf, "airline avg delay");
        jobAvgDelay.setJarByClass(AirlineDelayProcessor.class);
        jobAvgDelay.setMapperClass(AirlineMapper.class);
        jobAvgDelay.setReducerClass(AvgDelayReducer.class);
        jobAvgDelay.setOutputKeyClass(Text.class);
        jobAvgDelay.setOutputValueClass(Text.class);
        jobAvgDelay.setMapOutputKeyClass(Text.class);
        jobAvgDelay.setMapOutputValueClass(IntWritable.class);
        jobAvgDelay.addCacheFile(new Path(remainingArgs[1]).toUri());

        FileInputFormat.addInputPath(jobAvgDelay, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(jobAvgDelay, new Path(remainingArgs[2]));

        System.exit(jobAvgDelay.waitForCompletion(true) ? 0 : 1);
    }
}
