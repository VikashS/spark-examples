package spark.examples;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class Ex04_ParseCSV {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("ParseCSV");
		conf.set("spark.ui.port", "4141");
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc.addFile("/Users/aahme25/Data_Files/Line_of_numbers.csv");

		JavaRDD<String> lines = sc.textFile(SparkFiles.get("Line_of_numbers.csv"));

//		JavaRDD<String[]> numbersStrRDD = lines.map(new Function<String, String[]>() {
//			private static final long serialVersionUID = -2918134178396508726L;
//
//			public String[] call(String line) {
//				return line.split(",");
//			}
//		});

		JavaRDD<String[]> numbersStrRDD = lines.map(line -> line.split(","));
		
		List<String[]> val = numbersStrRDD.take(1);
		for (String[] e : val) {
			for (String s : e) {
				System.out.print(s + " ");
			}
			System.out.println();
		}
		
//		JavaRDD<String> strFlatRDD = lines.flatMap(new FlatMapFunction<String, String>() {
//			public Iterable<String> call(String line) {
//				return Arrays.asList(line.split(","));
//			}
//		});
		
		JavaRDD<String> strFlatRDD = lines.flatMap(line->Arrays.asList(line.split(",")));
				
		List<String> val1 = strFlatRDD.collect();
		for (String s : val1) {
			System.out.print(s + " ");
		}
		System.out.println();
		
//		JavaRDD<Integer> numbersRDD = strFlatRDD.map(new Function<String, Integer>() {
//			public Integer call(String s) {
//				return Integer.parseInt(s);
//			}
//		});
		
		JavaRDD<Integer> numbersRDD = strFlatRDD.map(s->Integer.parseInt(s));
		
		List<Integer> val2 = numbersRDD.collect();
		for (Integer s : val2) {
			System.out.print(s + " ");
		}
		System.out.println();
		
//		Integer sum = numbersRDD.reduce(new Function2<Integer, Integer, Integer>() {
//			public Integer call(Integer a, Integer b) {
//				return a + b;
//			}
//		});
		
		Integer sum = numbersRDD.reduce((x,y)->x+y);		
		System.out.println("Sum = " + sum);
		System.out.println("Sum2 = " + lines.flatMap(line->Arrays.asList(line.split(","))).map(s->Integer.parseInt(s)).reduce((x,y)->x+y));		
		sc.stop();
	}
}
