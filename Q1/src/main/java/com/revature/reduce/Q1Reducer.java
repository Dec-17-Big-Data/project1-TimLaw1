package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q1Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String[] vals = null;
		boolean isCountry = false;
		for (Text value : values) {
			String line = value.toString();
			if (line.length()<6) {
				if (line.equals("False")) {
					isCountry = false;
				} else {
					isCountry = true;
				}
			} else {
				vals = line.split(",");
			}
		}
		if (isCountry&&vals!=null) {
			for (int i = vals.length-1; i > 0; i--) {
				if (vals[i].equals("")) 
					continue;
				else {
					double myVal = Double.valueOf(vals[i]);
					if (myVal<30.0) {
						context.write(key, new DoubleWritable(myVal));
					}
					break;
				}
			}
		}
	}
}