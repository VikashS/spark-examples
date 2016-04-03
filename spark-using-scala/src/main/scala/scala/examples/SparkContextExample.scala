package scala.examples

import org.apache.spark.SparkContext
import scala.util.Properties

object SparkContextExample {
  def main(args: Array[String]): Unit = {
    val master = Properties.envOrElse("MASTER", "spark://PCW1844946L:7077")
    val sparkHome = Properties.propOrElse("SPARK_HOME", "")
    val myJars = Seq(java.lang.System.getenv(("JARS")))
    val sparkContext = new SparkContext(master, "my app", sparkHome, myJars)
    
    println(sparkContext.getConf.toDebugString)
    
    sparkContext.stop
  }
}