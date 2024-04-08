package indi.zijie.wcmr.charcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, LongWritable> {
    // 统计字符出现的次数，所以输出的键是字符，输出的值是次数
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将一行数据转换为字符数组
        char[] cs = value.toString().toCharArray();
        for (char c : cs) {
            context.write(new Text(c + ""), new LongWritable(1));
        }

    }

}
















