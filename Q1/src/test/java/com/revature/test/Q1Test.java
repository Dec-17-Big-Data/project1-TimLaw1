package com.revature.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Q1Mapper;
import com.revature.reduce.Q1Reducer;

public class Q1Test {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	private String key = "United States";
	private String inputString1 = "\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"29.5\",\"\",";
	private String inputString2 = "True";
	private String outputString1 = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,35.85857,37.8298,37.43131,38.22037,39.18913,39.84185,40.23865,41.26198,42.00725,42.78946,43.68347,,46.37914,47.68032,,,29.5,,";
	private double outputValue = 29.5;
	@Before
	public void setUp() {

		Q1Mapper mapper = new Q1Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		Q1Reducer reducer = new Q1Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {

		mapDriver.withInput(new LongWritable(1), new Text(inputString1));

		mapDriver.withOutput(new Text(key), new Text(outputString1));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {

		List<Text> values = new ArrayList<Text>();
		values.add(new Text(inputString2));
		values.add(new Text(outputString1));
		
		reduceDriver.withInput(new Text("USA"), values);

		List<Pair<Text,DoubleWritable>> myOutput;
		try {
			myOutput = reduceDriver.run();
			assert(Math.abs(myOutput.get(0).getSecond().get()-outputValue)<0.0001);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

//	@Test
//	public void testMapReduce() {
//
//		mapReduceDriver.withInput(new LongWritable(1), new Text(inputString1));
//
//		List<Pair<Text,DoubleWritable>> myOutput;
//		try {
//			myOutput = mapReduceDriver.run();
//			System.out.println(myOutput.get(0).getSecond().get());
//			assert(Math.abs(myOutput.get(0).getSecond().get()-outputValue)<0.0001);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
}

