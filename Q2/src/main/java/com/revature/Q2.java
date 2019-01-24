package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q2Mapper;
import com.revature.reduce.Q2Reducer;

/**
 * Hadoop Solution For Question 2 of Project 1 at Revature
 * Question 2: List the average increase in female education since the year 2000?
 * Solution Explanation:
 * 	A. Mapper
 * 		Find the line in USA with SE.TER.CUAT.BA.FE.ZS, output the key with the variable name and value of the entire line of Text input
 * 	B. Reducer
 * 		1. split the input value by csv comma
 * 		2. find first year >= 2000 and last year <=2016 with data
 * 		3. calculate average change over that time period using those 2 data points and the difference in years
 * 		4. output the average change to the reducer output
 * 	C. Post-analysis
 * 		TBD
 * Thought Process:
 * 	I wanted to find the average increase from 2000 to the most recent year. 
 * 	The year 2000 and the most recent year may be unavailable so I decided
 * 	to search the years in between those years in case they were empty data
 * 	points. 
 * Approach Applied to the problem:
 * 	I calculated the average by taking the difference in percentage 
 *  for female graduates and dividing that by the the difference in years.
 * Assumptions made:
 * 	I assumed that "graduates" in the question meant college graduates or higher 
 * 	because the project description mentioned "higher education" which usually
 * 	implies bachelor's and above.
 */	
public class Q2 {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(Q2.class);

		job.setJobName("Q2");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Q2Mapper.class);
		job.setReducerClass(Q2Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}