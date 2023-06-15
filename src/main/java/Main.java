import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.waiters.PollingStrategy;
import com.amazonaws.waiters.WaiterParameters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import utilities.MRFunctions;
import utilities.Names;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.evaluation.Prediction;
import weka.classifiers.functions.Logistic;
import weka.classifiers.trees.RandomForest;
import weka.classifiers.trees.lmt.LogisticBase;
import weka.core.Instances;

import java.io.*;
import java.util.Random;


public class Main {

    private static final AmazonS3 s3 = AmazonS3ClientBuilder
            .standard()
            .withRegion(Names.REGION)
            .build();


    private static void writeFeatures(Writer writer) throws  IOException {
        MRFunctions.getCount();
        int counter = 0;
        while (counter < MRFunctions.total) {
            writer.write("@attribute " + counter + "\tnumeric\n");
            counter++;
        }
    }

    private static void writeVectors(Writer writer) throws IOException {
        GetObjectRequest request = new GetObjectRequest(Names.NAME,"merged-output");
        S3Object object = s3.getObject(request);
        InputStreamReader streamReader = new InputStreamReader(object.getObjectContent());
        BufferedReader reader = new BufferedReader(streamReader);
        String line = reader.readLine();
        while (line != null) {
            writer.write(line + "\n");
            line = reader.readLine();
        }
        reader.close();
    }

    private static void train() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader("./classify.arff"));
        Instances train = new Instances(reader);
        reader.close();
        train.setClassIndex(train.numAttributes() - 1);
        RandomForest randomForest = new RandomForest();
        randomForest.buildClassifier(train);
        Evaluation eval = new Evaluation(train);
        eval.crossValidateModel(randomForest, train, 10,  new Random(1));
        System.out.println(eval.toSummaryString("\nResults\n======\n", true));
        System.out.println("FMeasure: " + eval.weightedFMeasure() + "\nPrecision: " + eval.weightedPrecision() + "\nRecall: " + eval.weightedRecall());
    }

    public static void main (String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Please enter DPMin value");
            return;
        }
        String dpMin = args[0];

        AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce awsMapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Names.REGION)
                .withCredentials(credentials)
                .build();

        HadoopJarStepConfig round1Config = config.hadoopStepConfig(Names.BUCKET + "Features.jar", new String[] {dpMin});
        StepConfig features = config.stepConfig("features", round1Config, "TERMINATE_JOB_FLOW");

        HadoopJarStepConfig round2Config = config.hadoopStepConfig(Names.BUCKET + "NounPairs.jar", new String[] {dpMin});
        StepConfig nounPairs = config.stepConfig("nounPairs", round2Config, "TERMINATE_JOB_FLOW");

        HadoopJarStepConfig round3Config = config.hadoopStepConfig(Names.BUCKET + "Hypernym.jar", new String[] {dpMin});
        StepConfig hypernym = config.stepConfig("hypernym", round3Config, "TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(3)
                .withMasterInstanceType(Names.INSTANCE_TYPE)
                .withSlaveInstanceType(Names.INSTANCE_TYPE)
                .withHadoopVersion("2.10.1")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withEc2KeyName(Names.KEY)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runJobFlowRequest = new RunJobFlowRequest()
                .withName("Ass3")
                .withReleaseLabel("emr-5.20.0")
                .withInstances(instances)
                .withSteps(features, nounPairs, hypernym)
                .withLogUri(Names.BUCKET + "log/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = awsMapReduce.runJobFlow(runJobFlowRequest);
        String jobID = runJobFlowResult.getJobFlowId();
        System.out.println("Started Job with id: " + jobID);
        System.out.println("Waiting on job flow to finish");
        while (true) {
            DescribeClusterResult describeClusterResult = awsMapReduce.describeCluster(new DescribeClusterRequest()
                    .withClusterId(jobID));
            Cluster cluster = describeClusterResult.getCluster();
            String state = cluster.getStatus().getState();
            if (state.equals("FAILED")) {
                System.out.println("Job Failed");
                return;
            } else if (state.equals("COMPLETED") || state.equals("TERMINATED")) {
                System.out.println("Job Flow Done");
                File file = new File("./classify.arff");
                file.createNewFile();
                FileWriter writer = new FileWriter("./classify.arff");
                writer.write("@relation hypernym\n");
                writeFeatures(writer);
                writer.write("@attribute class {True,False}\n");
                writer.write("@data\n");
                writeVectors(writer);
                writer.close();
                train();
                return;
            }
            Thread.sleep(5000);
        }
    }
}
