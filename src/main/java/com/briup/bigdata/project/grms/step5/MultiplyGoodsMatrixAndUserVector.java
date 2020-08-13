package com.briup.bigdata.project.grms.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		//调用run方法
		ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"step5");
		job.setJarByClass(this.getClass());
		
		MultipleInputs.addInputPath(job, new Path("/step3.1/part-r-00000"), TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
		MultipleInputs.addInputPath(job, new Path("/step4.1/part-r-00000"), TextInputFormat.class, MultiplyGoodsMatrixAndUserVectorSecondMapper.class);
		
		job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		job.waitForCompletion(true);
		return 0;
	}
}