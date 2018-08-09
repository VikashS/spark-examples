package mastering_spark_for_structured_streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.SparkSession

object Ex08_GroupBy extends App {
  case class Person(name: String, city: String, country: String, age: Option[Int])

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex08_GroupBy")
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

  peopleStream.groupBy('country)
    .mean("age")
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination(20000)
    
  peopleStream.groupBy('city)
    .agg(first("country") as "country" , count("age")) 
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination()
    
}