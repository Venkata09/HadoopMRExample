package com.yelp.stats;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;


/**
 *
 *
 * Top Yelp stats in PALO ALTO.
 *
 *
 *
 */
public class TopNStats {


    public static class FilterMapper extends Mapper<Object, Text, Text, Text> {
        private static BufferedReader br;
        private static Text rating = new Text();
        private static Text bussinessId = new Text();

        private static Map<String, String> bussinessDetails = new HashMap<>();

        protected void setUp(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getFileClassPaths();

            for (Path inputFilePath : cacheFiles) {
                searchCacheFile(inputFilePath);
            }
        }

        private void searchCacheFile(Path inputFilePath) throws IOException {
            String stringFromTheFile;

            br = new BufferedReader(new FileReader(inputFilePath.getName()));
            while ((stringFromTheFile = br.readLine()) != null) {
                String[] wordList = stringFromTheFile.split("::");
                if (wordList[1].toLowerCase().contains("palo alto")) {
                    bussinessDetails.put(wordList[0].trim(), wordList[1].trim());
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] ratingsData = value.toString().trim().split("::");

            if (bussinessDetails.containsKey(ratingsData[2].trim())) {
                bussinessId.set(ratingsData[2].trim());
                rating.set(ratingsData[3]);
                context.write(bussinessId, rating);
            }
        }

    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> ratings, Context context) throws IOException, InterruptedException {
            float count = 0;
            float sum = 0;

            for (Text rating : ratings) {
                sum += Float.parseFloat(ratings.toString());
                count++;
            }

            float avg = (sum / count);
            String avgString = String.valueOf(avg);
            context.write(new Text(key), new Text(avgString));
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "<<<<<<<<TopYelpReviewsInPaloAlto>>>>>>>>");
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        job.setJarByClass(TopNStats.class);
        job.setMapperClass(FilterMapper.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
