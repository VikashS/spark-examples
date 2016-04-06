package scala.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import au.com.bytecode.opencsv.CSVReader
import java.io.StringReader

object Ex03_LoadCsvWithCountersExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("ParseCSV")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)

    sc.addFile("/Users/aahme25/Data_Files/Line_of_numbers.csv")
    val inFile = sc.textFile(SparkFiles.get("Line_of_numbers.csv"))

    val invalidLineCounter = sc.accumulator(0)
    val invalidNumericLineCounter = sc.accumulator(0)

    val splitLines = inFile.flatMap(line => {
      try {
        val reader = new CSVReader(new StringReader(line))
        Some(reader.readNext())
      } catch {
        case _: Throwable => {
          invalidLineCounter += 1
          None
        }
      }
    })

    val numericData = splitLines.flatMap(line => {
      try {
        Some(line.map(_.toDouble))
      } catch {
        case _: Throwable => {
          invalidNumericLineCounter += 1
          None
        }
      }
    })

    val summedData = numericData.map(row => row.sum)
    println(summedData.collect().mkString(","))
    println("Errors: " + invalidLineCounter + "," + invalidNumericLineCounter)
  }
}