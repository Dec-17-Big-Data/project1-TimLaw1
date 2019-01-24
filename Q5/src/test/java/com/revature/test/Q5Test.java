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

import com.revature.map.Q5Mapper;
import com.revature.reduce.Q5Reducer;

public class Q5Test {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	private String inputString1 = "\"Afghanistan\",\"AFG\",\"Decision maker about a woman's own health care: mainly wife (% of women age 15-49)\",\"SG.DMK.HLTH.WF.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"4.8\",\"\",";
	private String outputString1 = "SG.DMK.HLTH.WF.ZS,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,4.8,,";
	private String inputString2 = "\"Afghanistan\",\"AFG\",\"GDP per capita (Current US$)\",\"NY.GDP.PCAP.CD\",\"59.7876806182323\",\"59.8900370439272\",\"58.5059949855131\",\"78.8025868928114\",\"82.2313944485018\",\"101.321627039285\",\"137.946782954494\",\"161.384701674071\",\"129.562318372974\",\"129.857378095621\",\"157.258460703612\",\"160.443151707287\",\"136.175611318523\",\"144.173944355103\",\"175.027097668921\",\"188.085136094081\",\"199.16480930329\",\"226.196426605461\",\"249.57370988274\",\"278.390630025236\",\"275.649818656436\",\"267.662423566787\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"119.899018754608\",\"192.153653041771\",\"203.650833206291\",\"224.914869067855\",\"257.175694454209\",\"280.245644106914\",\"380.400955186598\",\"384.131681276838\",\"458.955781585831\",\"569.940728793286\",\"622.379654358451\",\"690.842629014956\",\"653.347488111011\",\"633.947864294639\",\"594.323081219966\",\"\",";
	private String outputString2 = "NY.GDP.PCAP.CD,59.7876806182323,59.8900370439272,58.5059949855131,78.8025868928114,82.2313944485018,101.321627039285,137.946782954494,161.384701674071,129.562318372974,129.857378095621,157.258460703612,160.443151707287,136.175611318523,144.173944355103,175.027097668921,188.085136094081,199.16480930329,226.196426605461,249.57370988274,278.390630025236,275.649818656436,267.662423566787,,,,,,,,,,,,,,,,,,,,119.899018754608,192.153653041771,203.650833206291,224.914869067855,257.175694454209,280.245644106914,380.400955186598,384.131681276838,458.955781585831,569.940728793286,622.379654358451,690.842629014956,653.347488111011,633.947864294639,594.323081219966,,";
	private String inputString3 = "\"Afghanistan\",\"AFG\",\"Contraceptive prevalence, any methods (% of women ages 15-49)\",\"SP.DYN.CONU.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.6\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"4.9\",\"\",\"\",\"10.3\",\"\",\"13.6\",\"18.6\",\"\",\"22.8\",\"\",\"21.8\",\"21.2\",\"\",\"\",\"\",\"22.5\",\"\",";
	private String outputString3 = "SP.DYN.CONU.ZS,,,,,,,,,,,,,,1.6,,,,,,,,,,,,,,,,,,,,,,,,,,,4.9,,,10.3,,13.6,18.6,,22.8,,21.8,21.2,,,,22.5,,";
	private String outputKey1 = "Afghanistan";
	@Before
	public void setUp() {

		Q5Mapper mapper = new Q5Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		Q5Reducer reducer = new Q5Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	@Test
	public void testMapper() {

		mapDriver.withInput(new LongWritable(1), new Text(inputString3));

		mapDriver.withOutput(new Text(outputKey1), new Text(outputString3));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {

		List<Text> values = new ArrayList<Text>();
		values.add(new Text(outputString1));
		values.add(new Text(outputString2));
		values.add(new Text(outputString3));
		
		reduceDriver.withInput(new Text(outputKey1), values);
		List<Pair<Text, DoubleWritable>> output;
		try {
			output = reduceDriver.run();
//			for (Pair<Text,DoubleWritable> o: output) {
//				System.out.println(o.getFirst().toString());
//				System.out.println(o.getSecond().get());
//			}
			assert(Math.abs(output.get(0).getSecond().get()-2016)<0.001);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
//	@Test
//	public void testMapReduce() {
//		mapReduceDriver.addInput(new LongWritable(1), new Text(inputString1));
//		mapReduceDriver.addInput(new LongWritable(2), new Text(inputString2));
//		mapReduceDriver.addInput(new LongWritable(3), new Text(inputString3));
//		mapReduceDriver.addInput(key, val);
//		List<Pair<Text, DoubleWritable>> output;
//		try {
//			output = reduceDriver.run();
//			assert(Math.abs(output.get(0).getSecond().get()-2016)<0.001);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
}

