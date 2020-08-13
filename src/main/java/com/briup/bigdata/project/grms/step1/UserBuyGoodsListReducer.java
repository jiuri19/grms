package com.briup.bigdata.project.grms.step1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserBuyGoodsListReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		for(Text t : values){
			sb.append(t).append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		context.write(key, new Text(sb.toString()));
	}
}
