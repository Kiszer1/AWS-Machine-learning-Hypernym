package utilities;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.tartarus.snowball.ext.englishStemmer;

import java.io.*;
import java.net.URI;
import java.util.*;

public class MRFunctions {
    private static final englishStemmer stemmer = new englishStemmer();
    private static final Set<String> nounTags = new HashSet<>();
    private static final Set<String> indexes = new HashSet<>();
    public static final Map<String, Integer> features = new HashMap<>();
    public static int total = 0;


    private static final AmazonS3 s3 = AmazonS3ClientBuilder
            .standard()
            .withRegion(Names.REGION)
            .build();


    public static void tags() {
        nounTags.add("NN");
        nounTags.add("NNS");
        nounTags.add("NNP");
        nounTags.add("NNPS");
    }

    public static void indexes(Integer count) {
        while (count >= 0) {
            indexes.add(count.toString());
            count--;
        }
    }

    public static void features() throws  IOException {
        GetObjectRequest request = new GetObjectRequest(Names.NAME,"merged-features");
        S3Object object = s3.getObject(request);

        InputStreamReader streamReader = new InputStreamReader(object.getObjectContent());
        BufferedReader reader = new BufferedReader(streamReader);
        String line = reader.readLine();
        int position = 0;
        while (line != null) {
            features.put(line, position);
            position++;
            line = reader.readLine();
        }
    }

    public static void uploadCount(long total) {
        InputStream input = new ByteArrayInputStream((total + "").getBytes());
        ObjectMetadata data = new ObjectMetadata();
        data.setContentLength((total + "").getBytes().length);

        PutObjectRequest putRequest = new PutObjectRequest(Names.NAME,"features-count", input, data);
        s3.putObject(putRequest);
    }

    public static void getCount() throws  IOException {
        GetObjectRequest request = new GetObjectRequest(Names.NAME,"features-count");
        S3Object object = s3.getObject(request);

        InputStreamReader streamReader = new InputStreamReader(object.getObjectContent());
        BufferedReader reader = new BufferedReader(streamReader);

        total = Integer.parseInt(reader.readLine());
    }


    private static void getPath(String[] syntactic, List<String> nouns, String count, Mapper.Context context) throws IOException, InterruptedException {
        Set<Integer> seen = new HashSet<>();
        String path = "";

        for (String noun : nouns) {
            if (noun != null) {
                seen.clear();
                String[] split = noun.split("/");
                path = split[2];
                String nounPair = MRFunctions.stem(split[0].replaceAll("\\s", ""));
                int next = indexes.contains(split[3]) ? Integer.parseInt(split[3]) : 0;
                while (next != 0 && !seen.contains(next)) {
                    seen.add(next);
                    split = syntactic[next - 1].split("/");
                    String word = MRFunctions.stem(split[0].replaceAll("\\s", ""));
                    if (nounTags.contains(split[1]) && features.isEmpty()) {
                        context.write(new Text(path), new Text(nounPair + " " + word));
                    } else if (nounTags.contains(split[1]) && features.containsKey(path)) {
                        context.write(new Text(nounPair + " " + word), new Text(features.get(path) + " " + count));
                    }
                    path += "/" + word + "/" + split[2];
                    next = indexes.contains(split[3]) ? Integer.parseInt(split[3]) : 0;
                }
            }
        }
    }

    public static void checkSplit(String[] split, Mapper.Context context) throws IOException, InterruptedException{
        List<String> nouns = new ArrayList<>();
        if (split.length < 3 || split[1].split(" ").length < 3)
            return;
        String[] syntactic = split[1].split(" ");
        for (String tokens : syntactic) {
            String[] splitTokens = tokens.split("/");
            if (splitTokens.length < 4)
                return;
            if (nounTags.contains(splitTokens[1]))
                nouns.add(tokens);
        }
        if (nouns.size() >= 2)
            getPath(syntactic, nouns, split[2], context);
    }

    public static void saveHypernym(Map<String, String> hypernym) throws IOException {
        GetObjectRequest request = new GetObjectRequest(Names.NAME,"hypernym.txt");
        S3Object object = s3.getObject(request);

        InputStreamReader streamReader = new InputStreamReader(object.getObjectContent());
        BufferedReader reader = new BufferedReader(streamReader);
        String line = reader.readLine();
        while (line != null) {
            String[] split = line.split("\t");
            if (split.length >= 2) {
                String word1 = stem(split[0].replaceAll("\\s",""));
                String word2 = stem(split[1].replaceAll("\\s", ""));
                hypernym.put(word1 + " " + word2, split[2]);
            }
            line = reader.readLine();
        }
    }

    public static void mergeFiles(String inputDir, String outputDir, Configuration config) throws IOException {
        FileSystem local = FileSystem.get(URI.create(inputDir),config);
        FileSystem hdfs = FileSystem.get(URI.create(outputDir), config);
        Path inputDirPath = new Path(inputDir);
        Path outpuDirPath = new Path(outputDir);
        try {
            FileStatus[] inputFiles = local.listStatus(inputDirPath);
            OutputStream out = hdfs.create(outpuDirPath);
            for (int i = 0; i < inputFiles.length; i++) {
                System.out.println(inputFiles[i].getPath().getName());
                InputStream in = local.open(inputFiles[i].getPath());
                byte buffer[] = new byte[256];
                int bytesRead = 0;
                while( (bytesRead = in.read(buffer)) > 0)
                    out.write(buffer, 0, bytesRead);
                in.close();
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String stem(String str) {
        String next = "";
        String stemmed = str;
        while (!stemmed.equals(next)) {
            stemmer.setCurrent(stemmed);
            stemmer.stem();
            next = stemmed;
            stemmed = stemmer.getCurrent();
        }
        return stemmed;
    }
}
