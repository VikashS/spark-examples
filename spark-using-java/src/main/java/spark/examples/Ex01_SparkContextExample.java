package spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Ex01_SparkContextExample {

	public static void main(String[] args) {
		
		System.getenv().forEach((k,v)->System.out.println(k+" "+v));
		String master = System.getenv("MASTER");
		
		if (master==null) {
			master = "local";
		}
		
		String sparkHome = System.getenv("SPARK_HOME");
		if (sparkHome == null) {
			sparkHome = "./";
		}
		
		String jars = System.getenv("JARS");
		
		SparkConf conf = new SparkConf().setMaster(master).setAppName("My App").setSparkHome(sparkHome);
		conf.set("spark.ui.port", "4141");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		System.out.println(sc.getConf().toDebugString());
		
		sc.stop();
	}
}
