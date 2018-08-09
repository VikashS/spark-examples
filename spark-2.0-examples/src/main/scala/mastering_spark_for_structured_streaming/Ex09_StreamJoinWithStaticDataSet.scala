package mastering_spark_for_structured_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Ex09_StreamJoinWithStaticDataSet extends App {
  case class User(id: Int, name: String, email: String, country: String)
  case class Transaction(userId: Int, product: String, cost: Double)

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex09_StreamJoinWithStaticDataSet")
    .getOrCreate()

  import spark.implicits._

  val users = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("file:///Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/users.csv")
    .as[User]

  val transactionSchema = ScalaReflection.schemaFor[Transaction]
    .dataType
    .asInstanceOf[StructType]

  val transactionStream = spark.readStream
    .schema(transactionSchema)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .csv("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/transactions*")
    .as[Transaction]

  //Join transaction stream with user data
  val spendingByCountry = transactionStream
    .join(users, users("id") === transactionStream("userId"))
    .groupBy($"country")
    .agg(sum($"cost") as "spending")

  spendingByCountry
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination()
}