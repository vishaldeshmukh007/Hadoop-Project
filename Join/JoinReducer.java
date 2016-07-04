package com.mr.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class JoinReducer extends
					Reducer<IntWritable,Text, Text, Text> {
	
	private Text word = new Text();
	private Text tKey = new Text();
	private Text tValue = new Text();
	private Logger logger = Logger.getLogger(JoinReducer.class);

	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		logger.info("***** Reducer *****");
    	
		logger.info("key " + key.toString());
    	logger.info("value " + values.toString());
    	String sb = new String();
    	for (Text value : values) {
    		sb += value.toString()+",";
        }
    	tKey.set(key.toString());
    	tValue.set(sb.toString());
    	context.write( tKey, tValue);
        
	}
}
