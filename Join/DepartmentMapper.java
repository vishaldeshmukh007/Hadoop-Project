package com.mr.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class DepartmentMapper extends Mapper<LongWritable, Text, IntWritable,Text>{

	private Text word = new Text();
	private Logger logger = Logger.getLogger(DepartmentMapper.class);
	public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		logger.info(" Key "+ key.toString() + " ||  Value = " + value.toString());
		
		String entry = value.toString();
		String[] split = entry.split(",");
		if(split.length == 2){
			IntWritable deptNo = new IntWritable();
			word.set(split[1].trim().toString());
			deptNo.set(Integer.parseInt(split[0].trim()));
			logger.info(" DEPTNO " + deptNo.toString());
			logger.info(" NAME " + word);
			context.write(deptNo, word);
		}
		
	}
}
