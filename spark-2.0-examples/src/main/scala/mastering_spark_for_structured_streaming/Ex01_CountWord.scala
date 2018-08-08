package mastering_spark_for_structured_streaming

import org.apache.spark.sql.SparkSession
import scala.io.Source

object Ex01_CountWord extends App {
  val session = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex01_CountWord")
    .getOrCreate()

  val sc = session.sparkContext

  //Load Data
  val lines = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/othello.txt")

  //Count words
  val counts = (lines.flatMap(line => line.split("\\s+"))
    .map(w => (w.toLowerCase, 1))
    .reduceByKey(_ + _))

  //Sort and print top 20
  counts
    .sortBy(_._2, ascending = false)
    .take(20)
    .foreach(println)

  //Scala version
  val sLines = Source.fromFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/othello.txt").getLines

  val sCounts = sLines.flatMap(line => line.split("\\s+"))
    .toSeq
    .groupBy(_.toLowerCase())
    .mapValues(_.length)

  sCounts
    .toSeq
    .sortBy(_._2)
    .reverse
    .take(20)
    .foreach(println)

  session.close()
}