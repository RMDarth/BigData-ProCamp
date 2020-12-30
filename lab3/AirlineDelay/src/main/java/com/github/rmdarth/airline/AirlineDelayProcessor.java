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
                // GLC| it's ok to skip headers silently but it's better to log the entry out for debug purpose
                // GLC| it's better to create a counter for non-valid entries
                // GLC| Hint: The first parameter `key` is a file offset, check how many entries have it equal to `0`
                return; // skip header and empty delays

            try {
                airlineCode.set(attributes[4]);
                delayTime.set(Integer.parseInt(attributes[11]));
                context.write(airlineCode, delayTime);
            } catch (NumberFormatException ex) {
                // GLC| it's a good practice to count exceptions with Counters and avoid logging out each error
                // GLC| You can log out randomly with some chance (small one) or
                // GLC| emit a log entry per exception type / entry exra conditions
                // GLC| (say, NumberParseExc, entry has XX value) only once
                System.err.println("Can't parse delay time: " + ex.getMessage());
            }

            // GLC| There are default map counters which show the same info
            // GLC| It make more sense to create domain related counters (say, per airline)
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

        // GLC| Note: i haven't taken a look in the alternative job yet! :)
        // GLC| if you set the only reducer for the job
        // GLC| you can implement TopN algorithm right in here and write on Reducer.cleanup
        // GLC| so there is no need for extra MR jobs

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // GLC| It's better to use double in java
            float sum = 0;
            // GLC| It's better to long in this case
            float count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count ++;
            }

            // GLC| if there is no key in the map I'd use the code (`key`) value
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
        // GLC| It's worth adding a combiner but you cannot reuse AvgDelayReducer unless you modify it
        jobAvgDelay.setReducerClass(AvgDelayReducer.class);
        jobAvgDelay.setOutputKeyClass(Text.class);
        jobAvgDelay.setOutputValueClass(Text.class);
        jobAvgDelay.setMapOutputKeyClass(Text.class);
        jobAvgDelay.setMapOutputValueClass(IntWritable.class);
        jobAvgDelay.addCacheFile(new Path(remainingArgs[1]).toUri()); // to use for broadcast join

        // It's worth playing with compression
        // http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml
        // GLC| mapreduce.map.output.compress [false]	Should the outputs of the maps be compressed before being sent across the network. Uses SequenceFile compression.
        // GLC| mapreduce.map.output.compress.codec[org.apache.hadoop.io.compress.DefaultCodec]	If the map outputs are compressed, how should they be compressed?
        // GLC| conf.set("mapreduce.map.output.compress", true)
        // GLC| FileOutputFormat.setCompressOutput();
        // GLC| FileOutputFormat.setOutputCompressorClass(job, COMP_CLASS.class);
        FileInputFormat.addInputPath(jobAvgDelay, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(jobAvgDelay, new Path(remainingArgs[2]));

        System.exit(jobAvgDelay.waitForCompletion(true) ? 0 : 1);
    }
}
