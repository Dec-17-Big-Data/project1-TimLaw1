package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q5Mapper;
import com.revature.reduce.Q5Reducer;

/**
 * Hadoop Solution For Question 5 of Project 1 at Revature
 * Question 5: Based on contraceptive use and female health decision making stats, 
 * 				what developing countries are becoming more progressive quickest?
 * Solution Explanation:
 * 	A. Calculation
 * 		
 * 	B. Mapper
 * 		
 * 	C. Reducer
 * 		
 * 	D. Post-analysis
 * 		TBD
 * Thought Process:
 * 	I believe that a female's right to make her own health decisions and make an
 * 	educated decision on when to have children give her more freedom and liberty
 * 	to be involved in society instead of staying at home and raising kids.
 * Approach Applied to the problem:
 * 	I will take countries with a gdp per capita that indicates that it is a 
 * 	developing country. I will use 12,000 as the unofficial threshold for developing 
 * 	countries. I will then calculate the countries with the highest rate of having
 * 	their contraceptive needs met as well as rate of women who are in charge of making
 * 	their own health decisions.
 * Assumptions made:
 * 	Contraceptive availability and the ability to make their own health decisions is
 * 	a good indicator of female rights in a country.
 */
public class Q5 {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(Q5.class);

		job.setJobName("Q5");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Q5Mapper.class);
		job.setReducerClass(Q5Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}