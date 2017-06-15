package BookProcess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/**
 * Created by vdokku on 6/15/2017.
 */
public class BookMapper extends MapReduceBase implements
        org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable _key, Text value,
                    OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        String TempString = value.toString();
        String[] SingleBookData = TempString.split("\";\"");
        output.collect(new Text(SingleBookData[3]), one);
    }
}