package com.hadoop.simplejoin;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class studmap extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public void run(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String finalEntry=new String();
		Text word=new Text();
		IntWritable rollNo=new IntWritable();
		String temp=value.toString();
		String[] arr=temp.split(",");
		if(arr.length==3)
		{
			for(int i=0;i<=arr.length;i++)
			{
				if(i!=0)
				{
					finalEntry+=arr[i]+" ";
				}
				else
				{
					rollNo.set(Integer.parseInt(arr[0]));
				}
				word.set(finalEntry);
			}
			context.write(word, rollNo);
		}
	}
}
