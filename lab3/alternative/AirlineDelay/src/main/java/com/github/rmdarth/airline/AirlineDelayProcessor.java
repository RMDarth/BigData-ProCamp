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

                // GLC| There are default map counters which show the same info
                // GLC| It make more sense to create domain related counters (say, per airline)
                Counter counter = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.INPUT_FLIGHTS.toString());
                counter.increment(1);
            }
            catch (NumberFormatException e)
            {
                // GLC| it's a good practice to count exceptions with Counters and avoid logging out each error
                // GLC| You can log out randomly with some chance (small one) or
                // GLC| emit a log entry per exception type / entry exra conditions
                // GLC| (say, NumberParseExc, entry has XX value) only once
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
        // GLC| No need to declare the field member as it's used locally to loadAirlineNames
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
            // GLC| It's better to use double in java
            float sum = 0;
            // GLC| It's better to long in this case
            float count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count ++;
            }
            // GLC| What about key collisions? I'd use TreeSet
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
                // GLC| What would you need to change if you're asked to use `,` as the separator?
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
        // GLC| It's worth adding a combiner. It will speed up overall job execution
        jobAvgDelay.setReducerClass(AvgDelayReducer.class);
        jobAvgDelay.setOutputKeyClass(Text.class);
        jobAvgDelay.setOutputValueClass(Text.class);
        jobAvgDelay.setMapOutputKeyClass(Text.class);
        jobAvgDelay.setMapOutputValueClass(IntWritable.class);
        jobAvgDelay.addCacheFile(new Path(remainingArgs[1]).toUri());
        jobAvgDelay.setNumReduceTasks(1);

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
