package mastering_spark_for_structured_streaming

import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import java.io.OutputStreamWriter
import java.net.ServerSocket
import scala.io.Source

object Ex05_SocketStream2 extends App {

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex05_SocketStream2")
    .getOrCreate()

  import spark.implicits._

  val port = 12345

  (new Thread {
    override def run() {
      Broadcast.startServer(port)
    }
  }).start()

  def createStream(port: Int) {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .load()

    val words = lines
      .as[String]
      .flatMap(_.split("\\s+"))

    val wordCounts = words.coalesce(1)
      .groupByKey(_.toLowerCase)
      .count()
      .orderBy($"count(1)" desc)

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start
      .awaitTermination()
  }

  createStream(port)
}

object Broadcast {
  def startServer(port: Int) {
    val serverSocket = new ServerSocket(port)
    val socket = serverSocket.accept()
    val out: PrintWriter = new PrintWriter(new OutputStreamWriter(socket.getOutputStream))

    for (line <- Source.fromFile("/Users/aahmed/GitHub/spark-examples/spark-2.0-examples/src/main/resources/summer.txt").getLines) {
      println("read-> " + line)
      out.println(line.toString())
      out.flush()
      Thread.sleep(1000)
    }

    socket.close()
  }
}