package mastering_spark_for_structured_streaming

import org.apache.spark.sql.SparkSession

object Ex02_DataSet extends App {
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex01_CountWord")
    .getOrCreate()

  import spark.implicits._
  //Load data
  val text = spark.read
    .text("file:///Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/othello.txt")
    .as[String]

  val counts = text.flatMap(line => line.split("\\s+"))
    .groupByKey(_.toLowerCase)
    .count

  counts.printSchema
  counts.show

  counts.orderBy($"count(1)" desc).show
}