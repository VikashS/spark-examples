package spark_scala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source

object Ex08_MostPopularSuperHero extends App {

  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String) = {
    val fields = line.split("\\s+")
    (fields(0).toInt, fields.length - 1)
  }

  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String): Option[(Int, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
  //Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Ex08_MostPopularSuperHero").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  // Build up a hero ID -> name RDD
  val names = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/Marvel-names.txt")
  val namesRdd = names.flatMap(parseNames)

  // Load up the superhero co-apperarance data
  val lines = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/Marvel-graph.txt")

  // Convert to (heroID, number of connections) RDD
  val pairings = lines.map(countCoOccurences)

  // Combine entries that span more than one line
  val totalFriendsByCharacter = pairings.reduceByKey((x, y) => x + y)

  // Flip it to # of connections, hero ID
  val flipped = totalFriendsByCharacter.map(x => (x._2, x._1))

  // Find the max # of connections
  val mostPopular = flipped.max()

  // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
  val mostPopularName = namesRdd.lookup(mostPopular._2)(0)

  // Print out our answer!
  println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")

}