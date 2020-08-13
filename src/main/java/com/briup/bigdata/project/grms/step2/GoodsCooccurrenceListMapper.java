package com.briup.bigdata.project.grms.step2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GoodsCooccurrenceListMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String[] str = split[1].split("[,]");
		
		for(int i = 0;i<str.length;i++){
			for(int j = 0;j<str.length;j++){
				context.write(new Text(str[i]), new Text(str[j]));
			}
		}
	}
}
