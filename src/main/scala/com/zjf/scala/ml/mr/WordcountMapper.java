package com.zjf.scala.ml.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: zhangjianfeng
 * @Date: Created in 2018-12-06
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	/**
	 * map阶段的业务逻辑就写在自定义的map()方法中
	 * maptask会对每一行输入数据调用一次我们自定义的map()方法
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//将maptask传给我们的文本内容先转换成String
		String line = value.toString();
		//根据空格将这一行切分成单词
		String[] words = line.split(" ");
		
		//将单词输出为<单词，1>
		for(String word:words){
			//将单词作为key，将次数1作为value，以便于后续的数据分发，可以根据单词分发，以便于相同单词会到相同的reduce task
			context.write(new Text(word), new IntWritable(1));
		}
		
		
	}

}
