package spark_scala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.math.min

object Ex03_Filter extends App {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val station = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (station, entryType, temperature)
  }
  //Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Ex03_Filter").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  val lines = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/1800.csv")

  // Convert to (stationID, entryType, temperature) tuples
  val parsedLines = lines.map(parseLine)

  val filteredLines = parsedLines.filter(x => x._2 == "TMAX")

  // Convert to (stationID, temperature)
  val stationTemps = filteredLines.map(x => (x._1, x._3))

  // Reduce by stationID retaining the minimum temperature found
  val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y))

  // Collect, format, and print the results
  val results = minTempsByStation.collect()

  for (result <- results.sorted) {
    val station = result._1
    val temp = result._2
    val formattedTemp = f"$temp%.2f F"
    println(s"$station minimum temperature: $formattedTemp")
  }

}