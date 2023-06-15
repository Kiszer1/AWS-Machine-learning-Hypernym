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


public class Features {

    private enum Total {
        Counter
    }

    public static class FeaturesMap extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void setup(Context context) {
            MRFunctions.tags();
            MRFunctions.indexes(4);
        }

        @Override
        public void map(LongWritable file, Text line, Context context) throws InterruptedException, IOException {
            String[] split;
            split = line.toString().split("\t");
            MRFunctions.checkSplit(split, context);
        }
    }


    public static class FeaturesReducer extends Reducer<Text, Text, Text, Text> {

        private static int DPMin;

        @Override
        public void setup(Context context) {
            DPMin = Integer.parseInt(context.getConfiguration().get("DPMin"));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            Set<String> seen = new HashSet<>();
            for (Text value : values) {
                seen.add(value.toString());
                if (seen.size() > DPMin) {
                    context.write(key, null);
                    context.getCounter(Total.Counter).increment(1);
                    return;
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("DPMin", args[0]);
        Path input = new Path(Names.BUCKET + "data");
        Path output = new Path(Names.BUCKET + "features");
        Job job = Job.getInstance(conf, "Features");
        job.setJarByClass(Features.class);
        job.setMapperClass(FeaturesMap.class);
        job.setReducerClass(FeaturesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean done = job.waitForCompletion(true);

        MRFunctions.uploadCount(job.getCounters().findCounter(Total.Counter).getValue());

        String featuresOutput = Names.BUCKET + "/features/";
        String mergedFeatures = Names.BUCKET + "/merged-features";
        MRFunctions.mergeFiles(featuresOutput, mergedFeatures, conf);
        System.exit(done ? 0 : 1);
    }
}
