package mastering_spark_for_structured_streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.SparkSession

object Ex06_StreamWithSchema extends App {
  case class Person(name: String, city: String, country: String, age: Option[Int])

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex01_CountWord")
    .getOrCreate()

  import spark.implicits._

  //Create schema for parsing data
  val schema = ScalaReflection.schemaFor[Person]
    .dataType
    .asInstanceOf[StructType]

  val peopleStream = spark.readStream
    .schema(schema)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .csv("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/people*")
    .as[Person]

  peopleStream.writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()
}