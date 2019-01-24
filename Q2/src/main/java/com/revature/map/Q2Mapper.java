package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		
		if (line.contains("USA")) {
			if (line.contains("SE.TER.CUAT.BA.FE.ZS")) {	
				String[] vals = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1);
				String outKey = vals[3];
				outKey = outKey.replaceAll("\"", "");
				context.write(new Text(outKey), new Text(line));		
			}
		}
	}
}