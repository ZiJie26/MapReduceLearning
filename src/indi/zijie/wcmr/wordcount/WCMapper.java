package indi.zijie.wcmr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
        // hello tom hello bob
        String[] arr = ivalue.toString().split(" ");
        for (String str : arr) {
            context.write(new Text(str), new LongWritable(1));
        }

    }

}
