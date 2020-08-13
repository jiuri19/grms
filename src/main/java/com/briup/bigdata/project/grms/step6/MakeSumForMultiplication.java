package com.briup.bigdata.project.grms.step6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MakeSumForMultiplication extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MakeSumForMultiplication(), args));
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Path in = new Path("/step5.2/part-r-00000");
		Path out = new Path(conf.get("out"));
		
		Job job = Job.getInstance(conf,"step6");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(MakeSumForMultiplicationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.addInputPath(job, in);
		
		job.setReducerClass(MakeSumForMultiplicationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true)?0:1;
	}
	
	/*static class MakeSumForMultiplicationMapper extends Mapper<Text, Text, Text, Text>{
		
		private Text k2 = new Text();
		private Text v2 = new Text();
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			this.k2.set(split[0]);
			this.v2.set(split[1]);
			
			context.write(this.k2, this.v2);
		}
	}
	
	static class MakeSumForMultiplicationReducer extends Reducer<Text, Text, Text, Text>{
		
		private Text k3 = new Text();
		private Text v3 = new Text();
		private int sum = 0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			values.forEach((v)->{
				this.sum += Integer.parseInt(v.toString());
			});
			this.k3.set(key.toString());
			this.v3.set(String.valueOf(this.sum));
			context.write(this.k3, this.v3);
		}
	}*/

}
