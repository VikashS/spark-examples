package mastering_spark_for_structured_streaming

import org.apache.spark.sql.SparkSession

object Ex05_SocketStream extends App {

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex05_SocketStream")
    .getOrCreate()

  import spark.implicits._

  def createStream(port: Int, duration: Int) {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .load()

    val words = lines
      .as[String]
      .flatMap(_.split("\\s+"))

    val wordCounts = words
      .groupByKey(_.toLowerCase)
      .count()
      .orderBy($"count(1)" desc)

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start
      .awaitTermination()
  }

  createStream(12341, 10000)
}