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

import com.revature.map.Q2Mapper;
import com.revature.reduce.Q2Reducer;

public class Q2Test {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	private String inputTextString1 = "\"United States\",\"USA\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"31.39076\",\"32.00147\",\"32.67396\",\"\",";
	private String reduceKeyString1 = "SE.TER.CUAT.BA.FE.ZS";
	@Before
	public void setUp() {

		Q2Mapper mapper = new Q2Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		Q2Reducer reducer = new Q2Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {

		mapDriver.withInput(new LongWritable(1), new Text(inputTextString1));

		mapDriver.withOutput(new Text(reduceKeyString1), new Text(inputTextString1));
		
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {

		List<Text> values = new ArrayList<Text>();
		values.add(new Text(inputTextString1));

		reduceDriver.withInput(new Text("Q2"), values);
		
		List<Pair<Text, DoubleWritable>> output;
		try {
			output = reduceDriver.run();
			for (Pair<Text,DoubleWritable> o: output) {
				assert(Math.abs(o.getSecond().get()-0.6416)<0.0001);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testMapReduce() {

		mapReduceDriver.withInput(new LongWritable(1), new Text(inputTextString1));

		List<Pair<Text, DoubleWritable>> output;
		try {
			output = mapReduceDriver.run();
			for (Pair<Text,DoubleWritable> o: output) {
				assert(Math.abs(o.getSecond().get()-0.6416)<0.0001);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

