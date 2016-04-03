package spark.examples;

import org.apache.spark.api.java.JavaSparkContext;

public class SparkContextExample {

	public static void main(String[] args) {
		String master = System.getenv("MASTER");
		if (master == null) {
			master = "local";
		}
		String sparkHome = System.getenv("SPARK_HOME");
		if (sparkHome == null) {
			sparkHome = "./";
		}
		String jars = System.getenv("JARS");

		JavaSparkContext ctx = new JavaSparkContext(master, "my Java app", sparkHome , jars);
		
		System.out.println(ctx.getConf().toDebugString());
		
		ctx.stop();
	}

}
