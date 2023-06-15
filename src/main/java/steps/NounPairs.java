package steps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utilities.MRFunctions;
import utilities.Names;

import java.io.IOException;
import java.util.*;


public class NounPairs {

    public static class FeaturesMap extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void setup(Context context) throws IOException {
            MRFunctions.tags();
            MRFunctions.indexes(4);
            MRFunctions.features();
        }

        @Override
        public void map(LongWritable file, Text line, Context context) throws InterruptedException, IOException {
            String[] split;
            split = line.toString().split("\t");
            MRFunctions.checkSplit(split, context);
        }
    }


    public static class FeaturesReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            int newCount = 0;
            Map<String, Integer> feature = new HashMap<>();
            for (Text value : values) {
                String[] split = value.toString().split(" ");
                String position = split[0];
                int count = Integer.parseInt(split[1]);
                if (feature.containsKey(position))
                    newCount = count + feature.getOrDefault(position, 0);
                    feature.put(position, newCount);
            }

            if (feature.size() > 4) {
                String featureString = feature.toString();
                context.write(key, new Text(featureString.substring(1, featureString.length() - 1)));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(Names.BUCKET + "data");
        Path output = new Path(Names.BUCKET + "vectors");
        Job job = Job.getInstance(conf, "NounPairs");
        job.setJarByClass(NounPairs.class);
        job.setMapperClass(FeaturesMap.class);
        job.setReducerClass(FeaturesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean done = job.waitForCompletion(true);
        System.exit(done ? 0 : 1);
    }
}
