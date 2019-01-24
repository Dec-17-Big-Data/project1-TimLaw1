package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1);
		
		if (values.length<40&&values.length>10) {
			String outKey = values[2].replaceAll("\"","");
			String outValue;
			if (values[7]=="") {
				outValue = "False";
			} else {
				outValue = "True";
			}
			context.write(new Text(outKey),new Text(outValue));
		} else if (line.contains("SE.TER.CUAT.BA.FE.ZS")) {
			String outKey = values[0].replaceAll("\"","");
			String outValue = values[4].replaceAll("\"", "");
			for (int i = 5; i < values.length; i++) {
				if (i>4) { outValue = outValue.concat(",");}
				outValue = outValue.concat(values[i].replaceAll("\"", ""));
			}
			context.write(new Text(outKey), new Text(outValue));
		}
	}
}