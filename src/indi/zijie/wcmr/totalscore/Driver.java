/**
 *计算score.txt文件中每个人的总分
 */
package indi.zijie.wcmr.totalscore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    /**
     *
     * @param args
     * @throws Exception
     */
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
        job.setOutputValueClass(IntWritable.class);

        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.126.151:9000/czjinput/score.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.126.151:9000/czjoutput/totalscore/"));

        if (!job.waitForCompletion(true))
            return;
    }

}
