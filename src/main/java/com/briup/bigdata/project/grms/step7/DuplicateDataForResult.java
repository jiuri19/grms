package com.briup.bigdata.project.grms.step7;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DuplicateDataForResult extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DuplicateDataForResult(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		return 0;
	}
	
	static class DuplicateDataForResultFirstMapper extends Mapper<Text, Text, Text, Text>{
		
		private Text k2 = new Text();
		private Text v2 = new Text();
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			this.k2.set(split[0]);
			this.v2.set("a:"+split[1]);
			context.write(this.k2, this.v2);
		}
	}
	
	static class DuplicateDataForResultSecondMapper extends Mapper<Text, Text, Text, Text>{
		
		private Text k2 = new Text();
		private Text v2 = new Text();
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			this.k2.set(split[0]);
			this.v2.set("b:"+split[1]);
			context.write(this.k2, this.v2);
		}
	}
	
	static class DuplicateDataForResultReducer extends Reducer<Text, Text, Text, Text>{
		
		private String first = null;
		private List<String> list = new ArrayList<String>();
		private Text v3 = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for(Text t : values){
				String str = t.toString();
				if(str.indexOf("a:") != -1){
					this.first = str.replace("a:", "");
				}else if(str.indexOf("b:") != -1){
					this.list.add(str.replace("b:", ""));
				}
			}
			
			String[] split = this.first.split(",");
			
			this.list.forEach((str)->{
				for(int i = 0;i<split.length;i++){
					if(str.indexOf(split[i]) != -1){
						this.list.remove(str);
						break;
					}
				}
			});
			
			this.list.forEach((str)->{
				this.v3.set(str);
				try {
					context.write(key, this.v3);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			});
			
		}
	}
	
}