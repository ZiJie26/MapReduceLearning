package indi.zijie.wcmr.lettercount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/* letter
 *  other*/
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        char[] cs = value.toString().toCharArray();
        for (char c : cs) {
            if (c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z')
                context.write(new Text("letter"), new LongWritable(1));
            else
                context.write(new Text("other"), new LongWritable(1));
        }
    }
}
















