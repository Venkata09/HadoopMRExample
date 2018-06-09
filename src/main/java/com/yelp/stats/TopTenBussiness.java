package com.yelp.stats;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TopTenBussiness {

    public static class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {

        // Why protected ??
        protected void map(LongWritable lineNum, Text record, Context context) throws InterruptedException, IOException {
            String[] rec = record.toString().split("::");
            String bussinessId = rec[2];
            String rating = rec[3];

            context.write(new Text(bussinessId), new Text("reviews\t" + rating));
        }
    }

    public static class BusinessDetailsMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable lineNumber, Text record, Context context) throws InterruptedException, IOException {

            String[] rec = record.toString().split("::");
            String businessId = rec[0];
            String address = rec[1];
            String categories = rec[2];

            context.write(new Text(businessId), new Text("details\t" + address + "::" + categories));
        }
    }


    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> records, Context context)
                throws InterruptedException, IOException {

            float sum = 0;
            float count = 0;

            Text address = new Text();
            Text categories = new Text();
            Text businessId = key;

            for (Text rec : records) {
                String[] recSplit = rec.toString().split("\t");
                if (recSplit[0].equalsIgnoreCase("reviews")) {

                    sum += Float.parseFloat(recSplit[1]);
                    count++;
                } else if (recSplit[0].equalsIgnoreCase("details")) {
                    String[] details = recSplit[1].split("::");
                    address.set(details[0]);
                    address.set(details[1]);
                }
            }


            float avg = (sum / count);
            FloatWritable average = new FloatWritable();
            average.set(avg);
            Text joinResult = new Text();
            joinResult.set(address + "\t" + categories + "\t" + businessId + "::" + average);
            context.write(businessId, joinResult);
        }
    }

    public static class SortingMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String combinedRecord[] = value.toString().split("::");
            String[] details = combinedRecord[0].split("\t");
            String address = details[1];
            String categories = details[2];
            String biz_id = details[3];
            String average = combinedRecord[1];


            FloatWritable ratingsKey = new FloatWritable();
            ratingsKey.set(Float.parseFloat(average));

            String allDetails = address + "::" + categories + "::" + biz_id + "::" + average;
            context.write(ratingsKey, new Text(allDetails));

        }

    }


    // TOP Ten Business Ratings

    public static class TopTenBusinessReducer extends Reducer<FloatWritable, Text, Text, Text> {
        private int count = 0;

        // Reduce always receives the LIST of values and you have to iterate among those values.
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                String[] fields = v.toString().split("::");
                String addr = fields[0];
                String categories = fields[1];
                String bussinessId = fields[2];
                String avg = fields[3];


                if (count == 10) {
                    break;
                }
                context.write(new Text(bussinessId), new Text(addr + "\t" + categories + "\t" + avg));
                count++;
            }
        }
    }

    public static class SortFloatComparator extends WritableComparator {
        public SortFloatComparator() {
            super(FloatWritable.class, true);
        }

        public int compare(WritableComparable val1, WritableComparable val2) {
            FloatWritable key1 = (FloatWritable) val1;
            FloatWritable key2 = (FloatWritable) val2;
            // specify -1 for descending order
            return -1 * key1.compareTo(key2);
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // get all args
        if (args.length != 3) {
            System.err.println("Usage: TopTenBusinessDetails <reviews_dir> <business_details_dir> <out_dir> ");
            System.exit(2);
        }
        // join the two files
        Job job = new Job(conf, "joinOp");
        Path tempDir = new Path(args[2] + "_temp");
        job.setJarByClass(TopTenBussiness.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReviewMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BusinessDetailsMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, tempDir);
        job.waitForCompletion(true);

        // sort by average rating
        Job job2 = new Job(conf, "topTenBusinessDetails");
        job2.setJarByClass(TopTenBussiness.class);

        job2.setMapperClass(SortingMapper.class);
        job2.setReducerClass(TopTenBusinessReducer.class);

        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setSortComparatorClass(SortFloatComparator.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, tempDir);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

    }

}
