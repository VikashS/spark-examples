package scala.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import au.com.bytecode.opencsv.CSVReader
import java.io.StringReader
import org.apache.spark.SparkFiles

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ParseCSV")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)


    val pairRDD = sc.parallelize(Seq(("one",1),("two",2),("three",3)))
    
    sc.stop
  }
}