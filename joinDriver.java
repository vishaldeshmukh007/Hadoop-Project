package com.hadoop.simplejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class joinDriver 
{
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf=new Configuration();
		Path output=new Path("/result/join");
		Job job=new Job(conf,"mapreduce join");
		job.setJarByClass(joinDriver.class);
		Path path1=new Path("/umesh/join/marks.csv");
		MultipleInputs.addInputPath(job, path1, TextInputFormat.class,studmap.class);
		Path path2=new Path("/umesh/join/student.csv");
		MultipleInputs.addInputPath(job, path2, TextInputFormat.class,marksmap.class);
		job.setReducerClass(myreduce.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, output);
		FileSystem hdfs=FileSystem.get(conf);
		if(hdfs.exists(output))
		{
			hdfs.delete(output,true);
		}
		int code=job.waitForCompletion(true)? 0:1;
		System.exit(code);
	}
}
