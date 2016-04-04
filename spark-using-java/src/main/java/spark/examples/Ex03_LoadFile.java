package spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Ex03_LoadFile {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("LoadFile");
		conf.set("spark.ui.port", "4141");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		System.out.println("Running Spark Version : " +sc.version());
		sc.addFile("/Users/aahme25/Data_Files/spam.data");
		
		JavaRDD<String> lines = sc.textFile(SparkFiles.get("spam.data"));
		System.out.println(lines.first());
		
		sc.stop();
	}

}
