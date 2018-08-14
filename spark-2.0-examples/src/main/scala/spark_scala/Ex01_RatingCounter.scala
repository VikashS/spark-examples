package spark_scala

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Ex01_RatingCounter extends App {

  //Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Ex01_RatingCounter").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  // Load up each line of the ratings data into a RDD
  val lines = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/ml-100k/u.data")

  //Convert each line to a String, split it out by tabs, and extract the third field.
  //This file format is USERID, movieID, rating, timestamp
  val ratings = lines.map(l => l.toString.split("\t")(2))
  
  //Count up how many times each values (rating) occurs
  val results = ratings.countByValue()
  
  //Sort the resulting map of (rating, count) tuples
  val sortedResult = results.toSeq.sortBy(_._1)
  
  //print each result line by line
  sortedResult.foreach(println)
}