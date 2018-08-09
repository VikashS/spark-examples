package mastering_spark_for_structured_streaming

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Ex10_Sql extends App {
  case class Person(name: String, city: String, country: String, age: Option[Int])

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex10_Sql")
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

  peopleStream.createOrReplaceTempView("peopleTable")

  //SQL Query
  val query = spark.sql("select country, avg(age) from peopleTable group by country")

  query.writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination()

}