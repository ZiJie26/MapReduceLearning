package indi.zijie.wcmr.maxscore;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        IntWritable max = new IntWritable(0);
        /*
         迭代遍历
         IntWritable val = new IntWritable();
         val.set(684);
         val.get()>max.get();
         max = val; --对象赋值给的是地址， max 和 val 中的值一致
         第二次：val.set(340); --max 中的值也是变成340
         val.get()>max.get(); --false
         context.write(key, max); --此时max记录的是最后一个值。
         Bob 684 340 312 548
        */
        for (IntWritable val : values) {
            if (val.get() > max.get()) {
                // max = val;
                max.set(val.get());
            }
        }
        context.write(key, max);
    }
}


