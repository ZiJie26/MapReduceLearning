/**
 * characters.txt-统计这个文件中每一个字符出现的次数
 */
package indi.zijie.wcmr.charcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JobName");
        job.setJarByClass(Driver.class);
        // TODO: specify a mapper
        job.setMapperClass(Mapper.class);
        // TODO: specify a reducer
        job.setReducerClass(Reducer.class);

        // TODO: specify output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.126.151:9000/czjinput/characters.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.126.151:9000/czjoutput/charcount/"));

        if (!job.waitForCompletion(true))
            return;
    }

}
