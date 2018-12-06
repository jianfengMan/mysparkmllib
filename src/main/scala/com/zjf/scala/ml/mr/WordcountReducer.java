package com.zjf.scala.ml.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author: zhangjianfeng
 * @Date: Created in 2018-12-06
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{


	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int count=0;
		/*Iterator<IntWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			count += iterator.next().get();
		}*/
		
		for(IntWritable value:values){
		
			count += value.get();
		}
		
		context.write(key, new IntWritable(count));
		
	}
	
	
	
	
	
}
