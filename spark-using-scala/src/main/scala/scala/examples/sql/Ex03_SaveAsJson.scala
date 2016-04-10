package scala.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{ StructType, StructField, StringType };

object Ex03_SaveAsJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SaveAsJson")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rawRdd = sc.textFile("data/adult_10.txt")

    // specify a schema
    val schemaString = "age workclass fnlwgt education educational-num " +
      "marital-status occupation relationship race gender " +
      "capital-gain capital-loss hours-per-week native-country income"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // create row data
    val rowRDD = rawRdd.map(_.split(" ")).map(p => Row(p(0), p(1), p(2), p(3), p(4),
      p(5), p(6), p(7), p(8),
      p(9), p(10), p(11), p(12),
      p(13), p(14)))

    // create a data frame from schema and row data
    val adultDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // now save the data in json format
    val jsonData = adultDataFrame.toJSON

    jsonData.saveAsTextFile("/Users/aahme25/Data_Files/adult.json")
  }
}