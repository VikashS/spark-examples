package spark_scala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source

object Ex07_PopularMovieNicerUsingBC extends App {

  def loadMovies(): Map[Int, String] = {
    var movies: Map[Int, String] = Map()

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val lines = Source.fromFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/ml-100k/u.item").getLines

    for (l <- lines) {
      val fields = l.split('|')
      if (fields.length > 1) {
        movies += (fields(0).toInt -> fields(1))
      }
    }
    movies
  }

  //Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Ex07_PopularMovieNicerUsingBC").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  val baseRdd = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/ml-100k/u.data")

  // Create a broadcast variable of our ID -> movie name map
  val namesDict = sc.broadcast(loadMovies)

  // Map to (movieID, 1) tuples
  val movies = baseRdd.map(l => (l.split("\t")(1).toInt, 1))

  // Count up all the 1's for each movie
  val moviesCount = movies.reduceByKey((x, y) => x + y)

  // Flip (movieID, count) to (count, movieID)
  val flipped = moviesCount.map(x => (x._2, x._1))

  // Sort
  val sortedMovies = flipped.sortByKey()

  // Fold in the movie names from the broadcast variable
  val sortedMoviesWithNames = sortedMovies.map(x => (namesDict.value(x._2), x._1))

  // Collect and print results
  val results = sortedMoviesWithNames.collect()

  results.foreach(println)

}