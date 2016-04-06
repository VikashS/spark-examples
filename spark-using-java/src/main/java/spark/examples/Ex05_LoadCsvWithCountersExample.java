package spark.examples;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import au.com.bytecode.opencsv.CSVReader;

public class Ex05_LoadCsvWithCountersExample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Ex05_LoadCsvWithCountersExample");
		conf.set("spark.ui.port", "4141");
		JavaSparkContext sc = new JavaSparkContext(conf);

		final Accumulator<Integer> errors = sc.accumulator(0);
		
		sc.addFile("/Users/aahme25/Data_Files/Line_of_numbers.csv");
		JavaRDD<String> lines = sc.textFile(SparkFiles.get("Line_of_numbers.csv"));

//		JavaRDD<Integer[]> splitLines = lines.flatMap(new FlatMapFunction<String, Integer[]>() {
//			public Iterable<Integer[]> call(String line) {
//				ArrayList<Integer[]> result = new ArrayList<Integer[]>();
//				try {
//					CSVReader reader = new CSVReader(new StringReader(line));
//					String[] parsedLine = reader.readNext();
//					Integer[] intLine = new Integer[parsedLine.length];
//					for (int i = 0; i < parsedLine.length; i++) {
//						intLine[i] = Integer.parseInt(parsedLine[i]);
//					}
//					result.add(intLine);
//				} catch (Exception e) {
//					errors.add(1);
//				}
//				return result;
//			}
//		});
		
		
		JavaRDD<Integer[]> splitLines = lines.flatMap((line)->{
			ArrayList<Integer[]> result = new ArrayList<Integer[]>();
			try {
				CSVReader reader = new CSVReader(new StringReader(line));
				String[] parsedLine = reader.readNext();
				Integer[] intLine = new Integer[parsedLine.length];
				for (int i = 0; i < parsedLine.length; i++) {
					intLine[i] = Integer.parseInt(parsedLine[i]);
				}
				result.add(intLine);
				reader.close();
			} catch (Exception e) {
				errors.add(1);
			}
			return result;
		});
		
		
		List<Integer[]> res = splitLines.collect();
		System.out.print("Loaded data ");
		for (Integer[] e : res) {
			for (Integer val : e) {
				System.out.print(val + " ");
			}
			System.out.println();
		}
		System.out.println("Error count " + errors.value());

	}

}
