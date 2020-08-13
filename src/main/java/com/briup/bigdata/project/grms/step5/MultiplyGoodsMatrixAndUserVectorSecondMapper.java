package com.briup.bigdata.project.grms.step5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultiplyGoodsMatrixAndUserVectorSecondMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String str = split[1];
		str = "second:"+str;
		context.write(new Text(split[0]), new Text(str));
	}
}
