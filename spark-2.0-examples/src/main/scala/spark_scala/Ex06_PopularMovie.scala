package spark_scala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Ex06_PopularMovie extends App {

  //Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Ex06_PopularMovie").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  val baseRdd = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/ml-100k/u.data")

  // Map to (movieID, 1) tuples
  val movies = baseRdd.map(l => (l.split("\t")(1).toInt, 1))

  // Count up all the 1's for each movie
  val moviesCount = movies.reduceByKey((x, y) => x + y)

  // Flip (movieID, count) to (count, movieID)
  val flipped = moviesCount.map(x => (x._2, x._1))

  // Sort
  val sortedMovies = flipped.sortByKey()

  // Collect and print results
  val results = sortedMovies.collect()

  results.foreach(println)


}