package BookProcess;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



/**
 * Created by vdokku on 6/15/2017.
 */
public class BookReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text _key,
                       Iterator<IntWritable> values,
                       OutputCollector<Text,IntWritable> output,
                       Reporter reporter)
            throws IOException {
        // Getting the KEY.
        
        Text key = _key;
        int frequencyForYear = 0;
        while (values.hasNext()) {
            // replace ValueType with the real type of your value
            IntWritable value = (IntWritable) values.next();
            frequencyForYear += value.get();
            // process value
        }
        output.collect(key, new IntWritable(frequencyForYear));
    }
}