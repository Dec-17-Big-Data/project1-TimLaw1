package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q5Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
	
		if (line.contains("SG.DMK.HLTH.WF.ZS")||line.contains("NY.GDP.PCAP.CD")||line.contains("SP.DYN.CONU.ZS")) { 
			String[] vals = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1);
			String outKey = vals[0].replaceAll("\"","");
			String outValue = vals[3].replaceAll("\"", "");
			for (int i = 4; i < vals.length; i++) {
				if (i>3) { outValue = outValue.concat(",");}
				outValue = outValue.concat(vals[i].replaceAll("\"", ""));
			}
			context.write(new Text(outKey), new Text(outValue));
		}
	}
}