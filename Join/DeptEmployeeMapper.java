package com.mr.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class DeptEmployeeMapper extends Mapper<LongWritable, Text, IntWritable , Text>{
	private Text word = new Text();
	private Logger logger = Logger.getLogger(DeptEmployeeMapper.class);
	public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		logger.info(" Key "+ key.toString() + " ||  Value = " + value.toString());
		
		String entry = value.toString();
		logger.info(" ENTRY  " + entry);
		String[] split = entry.split(",");
		logger.info(" SPLIT length  " + split.length );
		if(split.length == 4){
			String updatedEntry = new String();
			IntWritable deptNo = null;
			for(int i = 0 ; i < split.length ; i++){
				if(i!=2)
					updatedEntry += split[i]+" ";
				else{
					int parseInt = Integer.parseInt(split[i]);
					deptNo = new IntWritable(parseInt);
				}	
			}
			word.set(updatedEntry);
			logger.info(" DEPTNO " + deptNo.toString());
			logger.info(" EMPLOYEE DETAILS " + word);
			context.write(deptNo, word);
		}
		
	}
	
}
