package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q4Mapper;
import com.revature.reduce.Q4Reducer;

/**
 * Hadoop Solution For Question 4 of Project 1 at Revature
 * Question 4: List the change in female employment since the year 2000
 * Solution Explanation:
 * 	A. Calculation
 * 		Take the most recent available data point from SL.EMP.TOTL.SP.FE.ZS and
 * 		the oldest since equal to or after 2000 and subtract the oldest from the
 * 		most recent.
 * 	B. Mapper
 * 		Finds SL.EMP.TOTL.SP.FE.ZS for each country after line 28890, which includes
 * 		everything including and after Afghanistan
 * 	C. Reducer
 * 		1. Search through the values for SL.EMP.TOTL.SP.FE.ZS from the most recent year
 * 			to the year 2001 and store the first value you come across
 * 		2. Do the same as 1 but from 2000 to 2015 and store that value
 * 		3. Subtract value from 2 from value from 1 and return that percentage difference
 * 	D. Post-analysis
 * 		TBD
 * Thought Process:
 * 	I wanted to find the change in employment percentage among females for each country
 *  from the year 2000 to the most recent year up to 2016.
 * Approach Applied to the problem:
 * 	I calculated the change in employment as the most recent available data point 
 * 	including and before 2016 in SL.EMP.TOTL.SP.FE.ZS minus the earliest data point
 *  including or after 2000.
 * Assumptions made:
 * 	I assumed that the question was asking for a list of percentage changes, one for each
 * 	country.
 */
public class Q4 {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(Q4.class);

		job.setJobName("Q4");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Q4Mapper.class);
		job.setReducerClass(Q4Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}