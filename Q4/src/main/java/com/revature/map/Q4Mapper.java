package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q4Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
	
		if (line.contains("SL.EMP.TOTL.SP.FE.ZS")) { 
			String[] vals1 = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1);
			context.write(new Text(vals1[0].replaceAll("\"", "")), new Text(line));
		}
	}
}