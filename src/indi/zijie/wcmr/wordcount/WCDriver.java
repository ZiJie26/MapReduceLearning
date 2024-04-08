package indi.zijie.wcmr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 *计算文件words.txt中每个单词出现的次数
 */
public class WCDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(WCDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(WCMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(WCReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.126.151:9000/czjinput/words.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.126.151:9000/czjoutput/words/"));

		if (!job.waitForCompletion(true))
			return;
	}

}
