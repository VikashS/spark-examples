package mastering_spark_for_structured_streaming

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec

object Ex12_DStreamStateFul extends App {
  val filename = "/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/summer.txt"
  class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
    def onStart() {
      new Thread("Timed File Source") {
        override def run() = { receive() }
      }.start()
    }
    def onStop() {}

    private def receive() = {
      for (line <- Source.fromFile(filename).getLines) {
        println(line) // print for debugging
        store(line) // send the line as source
        Thread.sleep(1000) //wait one second
      }
    }
  }

  def updateFn(key: String, value: Option[Int], state: State[Int]) = {

    // update state (stateful)
    state.update(value.getOrElse(0) + state.getOption.getOrElse(0))

    (key, state.get)
  }

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex12_DStreamStateFul")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val spec = StateSpec.function(updateFn _)

  ssc.checkpoint("_checkpoint")

  ssc.receiverStream(new CustomReceiver())
    .flatMap(_.split(" "))
    .map(w => (w.toLowerCase(), 1))
    .mapWithState(spec)
    .print()

  ssc.start()
  ssc.awaitTermination()

}