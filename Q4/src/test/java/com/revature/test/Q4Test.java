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

import com.revature.map.Q4Mapper;
import com.revature.reduce.Q4Reducer;

public class Q4Test {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	private String inputString1 = "\"Uruguay\",\"URY\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"65.0500030517578\",\"\",\"\",\"\",\"\",\"\",\"55.25\",\"68.0899963378906\",\"67.6900024414063\",\"67.120002746582\",\"67.9100036621094\",\"68.0500030517578\",\"\",\"\",\"67.2699966430664\",\"\",\"64.0800018310547\",\"63.8899993896484\",\"\",\"59.7099990844727\",\"\",\"62.6699981689453\",\"67.0100021362305\",\"69.0500030517578\",\"69.1999969482422\",\"70.3199996948242\",\"69.2799987792969\",\"71.0400009155273\",\"69.879997253418\",\"70.1600036621094\",\"70.5500030517578\",\"69.6800003051758\",\"\",";
	private String outputString1 = inputString1;
	private String outputKey1 = "Uruguay";
	@Before
	public void setUp() {

		Q4Mapper mapper = new Q4Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		Q4Reducer reducer = new Q4Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	@Test
	public void testMapper() {

		mapDriver.withInput(new LongWritable(28890), new Text(inputString1));

		mapDriver.withOutput(new Text(outputKey1), new Text(outputString1));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {

		List<Text> values = new ArrayList<Text>();
		values.add(new Text(inputString1));
		
		reduceDriver.withInput(new Text(outputKey1), values);
		List<Pair<Text, DoubleWritable>> output;
		try {
			output = reduceDriver.run();
			assert(Math.abs(output.get(0).getSecond().get()-5.5999984741211)<0.0001);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(28890), new Text(inputString1));
		List<Pair<Text, DoubleWritable>> output;
		try {
			output = mapReduceDriver.run();
			assert(Math.abs(output.get(0).getSecond().get()-5.5999984741211)<0.0001);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

