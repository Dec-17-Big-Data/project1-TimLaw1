package com.revature.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q5Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double yearIndex = 100.0;
		double GDP=0.0,CUR=0.0,FCR=0.0;
		for (Text value : values) {
			int yeari = 0;
			String[] vals = value.toString().split(",");
			for (int i = vals.length-1; i>0; i--) {
				if (vals[i].equals("")) {
					continue;
				} else {
					yeari = i;
					if (yeari<yearIndex) {
						yearIndex = yeari;
					}
					break;
				}
			}
			if (yeari==0) {
				continue;
			}
			double output = Double.valueOf(vals[yeari]);
			if (vals[0].equals("SG.DMK.HLTH.WF.ZS")) {
				FCR=output;
			} else if (vals[0].equals("NY.GDP.PCAP.CD")) {
				GDP=output;
			} else if (vals[0].equals("SP.DYN.CONU.ZS")) { 
				CUR=output;
			}
		}
		double minYear = 1960 + yearIndex;
		if ((Math.abs(GDP-0.0)>0.0001)&&(Math.abs(CUR-0.0)>0.0001)&&(Math.abs(FCR-0.0)>0.0001)) {
			if (GDP<12000) {
				context.write(new Text(key.toString()+"minYear"), new DoubleWritable(minYear));
				context.write(new Text(key.toString()+"FCR"), new DoubleWritable(FCR));
				context.write(new Text(key.toString()+"CUR"), new DoubleWritable(CUR));
				context.write(new Text(key.toString()+"GDP"), new DoubleWritable(GDP));
			}
		}
	}
}