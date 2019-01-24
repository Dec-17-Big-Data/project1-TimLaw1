package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q1Mapper;
import com.revature.reduce.Q1Reducer;

/**
 * Hadoop Solution For Question 1 of Project 1 at Revature
 * Question 1: Identify countries where the percentage of female graduates is less than 30%.
 * Solution Explanation:
 * 	A. Calculate
 * 		*We will use SE.TER.CUAT.BA.FE.ZS for the female graduation rate
 * 	B. Mapper
 * 		Returns a key with the country of the input line, and a sequence of values
 * 		for the input line that are it's columns from 1960 to 2016 data
 * 	C. Reducer
 * 		Outputs the country key and current female graduation rate if it is lower
 * 		than 30 
 * 	D. Post-analysis
 * 		TBD
 * 	E. Future Improvements
 * 		Cleanse the data with map reduce so that I only have data for countries
 * Thought Process:
 * 	I wanted to find the countries with a female graduation rate lower than 30%.
 * Approach Applied to the problem:
 * 	I calculated the graduation rate as the most recent available graduation rate
 * 	including all years from 1960-2016.
 * Assumptions made:
 * 	I assumed that "graduates" in the question meant college graduates or higher.
 */
public class Q1 {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(Q1.class);

		job.setJobName("Q1");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Q1Mapper.class);
		job.setReducerClass(Q1Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}