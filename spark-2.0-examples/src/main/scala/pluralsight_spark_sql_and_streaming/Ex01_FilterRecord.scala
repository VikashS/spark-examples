package pluralsight_spark_sql_and_streaming

import org.apache.spark.sql.{ DataFrame, SparkSession, SaveMode }

object Ex01_FilterRecord extends App {
  val spark = SparkSession
    .builder()
    .appName("Ex01_FilterRecord")
    .master("local[2]")
    .config("spark.driver.memory", "128m")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val financeDF = spark.read.json("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/finances-small.json")

  financeDF
    .na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date")) //(na-NotAvailable) - Handle the scenario of missing data. Drop rows where all columns are null or NaN. Clearing out empty rows
    .na.fill("UnKnown", Seq("Description")) //
    .where(($"Amount" =!= 0) || $"Description" === "UnKnown")
    .selectExpr("Account.Number as AccountNumber", "Amount", "Date", "Description")
    .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")

  if (financeDF.hasColumn("_corrupt_record")) {
    financeDF.where($"_corrupt_record".isNotNull)
      .select($"_corrupt_record")
      .write.mode(SaveMode.Overwrite).parquet("Output/corrupt_finances")
  }
  implicit class DataFrameHelper(df: DataFrame) {
    import scala.util.Try

    def hasColumn(columnName: String) = Try(df(columnName)).isSuccess
  }
  
  //Other option to drop corrupt record
  //spark.read
  //    .option("mode", "DROPMALFORMED").json("[PATH]")
  //                  , "FAILFAST"
  //                  , "PERMISSIVE"
}