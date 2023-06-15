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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class HypernymMR {

    public static class HypernymMap extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, String> hypernym = new HashMap<>();


        @Override
        public void setup(Context context) throws IOException {
            MRFunctions.saveHypernym(hypernym);
        }

        @Override
        public void map(LongWritable file, Text line, Context context) throws InterruptedException, IOException {
            String[] split;
            split = line.toString().split("\t");
            if (hypernym.containsKey(split[0])) {
                context.write(line, new Text(hypernym.get(split[0])));
            }
        }
    }


    public static class HypernymReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void setup(Context context) throws IOException {
            MRFunctions.getCount();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            String isHypernym = values.iterator().next().toString();
            String [] split = key.toString().split("\t");
            String [] elements = split[1].split(", ");
            int[] pairFeatures = new int[MRFunctions.total];

            for (String element : elements) {
                String[] splitElement = element.split("=");
                int position = Integer.parseInt(splitElement[0]);
                int count = Integer.parseInt(splitElement[1]);
                pairFeatures[position] += count;

            }
            String featuresString = Arrays.toString(pairFeatures);
            featuresString = featuresString.substring(1, featuresString.length() - 1);
            context.write(new Text(featuresString + "," + isHypernym), null);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(Names.BUCKET + "vectors");
        Path output = new Path(Names.BUCKET + "output");
        Job job = Job.getInstance(conf, "HypernymMR");
        job.setJarByClass(HypernymMR.class);
        job.setMapperClass(HypernymMap.class);
        job.setReducerClass(HypernymReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean done = job.waitForCompletion(true);

        String outputs = Names.BUCKET + "/output/";
        String mergedOutput = Names.BUCKET + "/merged-output";
        MRFunctions.mergeFiles(outputs, mergedOutput, conf);

        System.exit(done ? 0 : 1);
    }
}
