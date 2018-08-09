package mastering_spark_for_structured_streaming

import org.apache.spark.sql.SparkSession

object Ex13_TungstenPerformanceDemo extends App {

  //The following two operations count a million integers in memory.
  //  - The first uses RDDs and must serialize all the objects into Java Objects.
  //  - The second uses Tungsten and uses a more compact binary encoding.

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex13_TungstenPerformanceDemo")
    .getOrCreate()

  import spark.implicits._

  val million = spark.sparkContext.parallelize(0 until math.pow(10, 6).toInt)

  //using RDDS
  
  million.cache.count

  //using tungsten
  million.toDS.cache.count
}