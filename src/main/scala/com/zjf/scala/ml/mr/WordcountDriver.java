package com.zjf.scala.ml.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description:
 * @Author: zhangjianfeng
 * @Date: Created in 2018-12-06
 */
public class WordcountDriver {
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		//是否运行为本地模式，就是看这个参数值是否为local，默认就是local
		conf.set("mapreduce.framework.name", "local");
		
		//本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
		//到底在哪里，就看以下两行配置你用哪行，默认就是file:///
//		conf.set("fs.defaultFS", "hdfs://linux01:9000/");
		conf.set("fs.defaultFS", "file:///");
		
		
		
		//运行集群模式，就是把程序提交到yarn中去运行
		//要想运行为集群模式，以下3个参数要指定为集群上的值
	/*	conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "linux01");
		conf.set("fs.defaultFS", "hdfs://linux01:9000/");*/
		Job job = Job.getInstance(conf);
		
//		job.setJar("c:/wc.jar");
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(WordcountDriver.class);
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//指定需要使用combiner，以及用哪个类作为combiner的逻辑
		/*job.setCombinerClass(WordcountCombiner.class);*/
		job.setCombinerClass(WordcountReducer.class);
		
		//如果不设置InputFormat，它默认用的是TextInputformat.class
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

		//指定job的输入原始文件所在目录
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path("/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/MR/wordcount.txt"));
//		指定job的输出结果所在目录
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/MR/result"));

		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/*job.submit();*/
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
