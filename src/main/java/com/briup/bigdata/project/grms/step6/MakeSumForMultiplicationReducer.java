package com.briup.bigdata.project.grms.step6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MakeSumForMultiplicationReducer extends Reducer<Text, Text, Text, Text>{
	
	private int sum = 0;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		for(Text t:values){
			this.sum += Integer.parseInt(t.toString());
		}
		
		context.write(key, new Text(String.valueOf(this.sum)));
	}
}
