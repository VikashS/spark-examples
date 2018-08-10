package pluralsight_spark_sql_and_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object Ex02_FilterRecord2 extends App {
  val spark = SparkSession
    .builder()
    .appName("Ex02_FilterRecord2")
    .master("local[2]")
    .config("spark.driver.memory", "128m")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  spark.read.json("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/finances-small.json")
    .na.drop("all")    //(na-NotAvailable) - Handle the scenario of missing data. Drop rows where all columns are null or NaN. Clearing out empty rows 
    .na.fill("UnKnown", Seq("Description"))  //
    .where(($"Amount" =!= 0) || $"Description" === "UnKnown")
    .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")
}