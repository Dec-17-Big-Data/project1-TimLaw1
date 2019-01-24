package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Q2Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String line = value.toString();
			String[] vals = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1);
			String[] preDoubleVals = new String[vals.length-4];
			for (int i = 4; i<vals.length; i++) {
				String tempString = vals[i];
				tempString = tempString.replaceAll("\"", "");
				preDoubleVals[i-4] = tempString;
			}
			Double[] doubleVals = new Double[preDoubleVals.length];
			for (int i = 0; i < preDoubleVals.length; i++) {
				if (preDoubleVals[i].equals("")) {
					doubleVals[i] = 0.0;
				} else {
					doubleVals[i] = Double.valueOf(preDoubleVals[i]);	
				}
			}
			double maxYearValue = 0, minYearValue = 0;
			int minYearIndex = 0, maxYearIndex = 0;
			for (int i = 39; i < doubleVals.length-1; i++) {
				if (Math.abs(doubleVals[i]-0.0)>0.0001) {
					minYearValue = doubleVals[i];
					minYearIndex = i;
					break;
				}
			}
			for (int i = doubleVals.length-1; i > 39; i--) {
				if (Math.abs(doubleVals[i]-0.0)>0.0001) {
					maxYearValue = doubleVals[i];
					maxYearIndex = i;
					break;
				}
			}
			double minYear = 1961+minYearIndex;
			double maxYear = 1961+maxYearIndex;
			double percentChange = maxYearValue - minYearValue;
			double averageChange = percentChange / (maxYearIndex-minYearIndex); 
			context.write(new Text(key+"averageIncrease"), new DoubleWritable(averageChange)); 
		}
	}
}