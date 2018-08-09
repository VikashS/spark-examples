package mastering_spark_for_structured_streaming

import org.apache.spark.sql.SparkSession

object Ex04_CountStream extends App {
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex04_CountStream")
    .getOrCreate()

  import spark.implicits._

  //Load Data
  val text = (spark.readStream
    .option("maxFilesPerTrigger", 1)
    .text("file:///Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/part*")
    .as[String])

  //Count words
  val counts = (text.flatMap(line => line.split("\\s+"))
    .groupByKey(_.toLowerCase)
    .count)

  //Print counts
  val query = (counts
    .orderBy($"count(1)" desc)
    .writeStream
    .outputMode("complete")
    .format("console")
    .start)

  query.awaitTermination()
}