package com.briup.bigdata.project.grms.step3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GoodsCooccurrenceMatrixReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Map<String,Integer> map = new HashMap<>();
		for(Text t : values){
				if(map.containsKey(t.toString())){
					Integer integer = map.get(t.toString())+1;
					map.put(t.toString(), integer);
				}else{
					map.put(t.toString(), 1);
				}
		}
		
		StringBuffer sb = new StringBuffer();
		
		map.forEach((s,i)->{
			
			sb.append(s).append(":");
			sb.append(i).append(",");
		});
		
		sb.deleteCharAt(sb.length()-1);
		
		context.write(key, new Text(sb.toString()));
	}
}
