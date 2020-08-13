package com.briup.bigdata.project.grms.step4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserBuyGoodsVectorMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String[] split2 = split[1].split(",");
		
		for(int i = 0;i<split2.length;i++){
			context.write(new Text(split2[i]),new Text(split[0]));
		}
	}
}
