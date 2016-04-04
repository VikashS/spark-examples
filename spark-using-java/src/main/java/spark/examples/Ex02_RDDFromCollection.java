package spark.examples;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Ex02_RDDFromCollection {

	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("RDDFromCollection").setMaster("local");
		conf.set("spark.ui.port", "4141");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1,2,3));
		System.out.println(dataRDD.count());
		System.out.println(dataRDD.take(3));
		sc.stop();
	}
}
