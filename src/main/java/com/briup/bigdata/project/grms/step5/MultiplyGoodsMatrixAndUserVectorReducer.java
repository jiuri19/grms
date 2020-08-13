package com.briup.bigdata.project.grms.step5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiplyGoodsMatrixAndUserVectorReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//Map<String, List<Integer>> map = new HashMap<String, List<Integer>>();
		Map<String, List<Integer>> map = new TreeMap<String, List<Integer>>(new Comparator<String>(){

			@Override
			public int compare(String o1, String o2) {
				String[] o1s = o1.split(",");
				String[] o2s = o2.split(",");
				int compareTo = o1s[0].compareTo(o2s[0]);
				if(compareTo > 0){
					return 1;
				}else if(compareTo < 0){
					return -1;
				}else if(compareTo == 0){
					return o1s[1].compareTo(o2s[1]);
				}
				return 0;
			}
			
		});
		String[] first = null;
		String[] second = null;
		
		int a = 0;
		int b = 0;
		int end = 0;
		
		
		//String endstr = null;
		
		for(Text t : values){
			String vstr = t.toString();
			if(vstr.indexOf("first:") != -1){
				first = vstr.replace("first:", "").split("[,]");
			}else if(vstr.indexOf("second:") != -1){
				second = vstr.replace("second:", "").split("[,]");
			}
		}
		
		for(int i = 0;i<second.length;i++){
			String[] split = second[i].split("[:]");
			a = Integer.parseInt(split[1]);
			for(int j = 0;j<first.length;j++){
				String[] split2 = first[i].split("[:]");
				//UserAndGoods start = new UserAndGoods(new Text(split[0]),new Text(split2[0]));
				String str = split[0]+","+split2[0];
				b = Integer.parseInt(split2[1]);
				end = a*b;
				if(map.containsKey(str)){
					map.get(str).add(end);
				}else{
					List<Integer> list = new ArrayList<Integer>();
					list.add(end);
					map.put(str,list);
				}
				
				//endstr = String.valueOf(end);
				//context.write(start, new Text(endstr));
			}
		}
		
		map.forEach((mapk,mapv)->{
			mapv.forEach((listv)->{
				try {
					context.write(new Text(mapk), new Text(String.valueOf(listv)));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			});
		});
		
	}
}
