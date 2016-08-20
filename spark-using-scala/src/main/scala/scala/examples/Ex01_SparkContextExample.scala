package scala.examples

import scala.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Ex01_SparkContextExample {
  def main(args: Array[String]): Unit = {
    val master = Properties.envOrElse("MASTER", "local")
    val myJars = Seq(java.lang.System.getenv(("JARS")))

    val conf = new SparkConf().setMaster(master).setAppName("My App")
    conf.setJars(myJars)
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)

    println(sc.getConf.toDebugString)

    sc.stop
  }
}