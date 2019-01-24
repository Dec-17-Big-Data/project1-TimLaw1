package com.revature.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q3Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Iterator<Text> j = values.iterator(); values.iterator().hasNext();) {
			String line1 = j.next().toString();
			String[] vals1 = line1.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1);
			for (int i = 0; i < vals1.length;i++) {
				vals1[i] = vals1[i].replaceAll("\"", "");
			}
			double newestPercentageMaleEmployed=0.0;
			int newestYear = 0;
			for (int i = vals1.length-1; i>43;i--) {
				if (vals1[i].equals("")) { continue; } 
				newestYear = i;
				newestPercentageMaleEmployed = Double.valueOf(vals1[i]);
				break;
			}
			double oldestPercentageMaleEmployed=0.0;
			int oldestYear = 0;
			for (int i = 43; i<vals1.length-1;i++) {
				if (vals1[i].equals("")) { continue; }
				oldestYear = i;
				oldestPercentageMaleEmployed = Double.valueOf(vals1[i]);
				break;
			}
			if (!(Math.abs(oldestPercentageMaleEmployed-0.0)<0.0001)&&!(Math.abs(newestPercentageMaleEmployed-0.0)<0.0001)&&(newestYear!=oldestYear)) {
				double percentageChange = newestPercentageMaleEmployed-oldestPercentageMaleEmployed;
				context.write(new Text(key.toString()+" percentage change"), new DoubleWritable(percentageChange));
			}
		}
	}
}