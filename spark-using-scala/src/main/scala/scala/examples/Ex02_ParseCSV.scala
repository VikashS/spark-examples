package scala.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import au.com.bytecode.opencsv.CSVReader
import java.io.StringReader
import org.apache.spark.SparkFiles

object Ex02_ParseCSV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ParseCSV")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)

    sc.addFile("data/Line_of_numbers.csv")
    val inFile = sc.textFile(SparkFiles.get("Line_of_numbers.csv"))

    val splitLines = inFile.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }

    val numericData = splitLines.map(line => line.map(_.toDouble))

    val summedData = numericData.map(row => row.sum)

    println(summedData.collect().mkString(","))

    sc.stop
  }
}