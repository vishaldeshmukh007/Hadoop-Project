package com.hadoop.simplejoin;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myreduce extends Reducer<IntWritable, Text, Text, Text>
{
	public void run(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Text finalkey=new Text();
		Text finalvalue=new Text();
		String result=new String();
		for(Text value : values)
		{
			result += value.toString()+ " ";
		}
		finalkey.set(key.toString());
		finalvalue.set(result.toString());
		context.write(finalkey, finalvalue);
	}
}
