package spark_scala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Ex04_FlatMap extends App {
  //Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Ex04_FlatMap").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  val lines = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/book.txt")

  // Split into words separated by a space character
  val words = lines.flatMap(line => line.split("\\W+"))

  // Normalize everything to lowercase
  val lowercaseWords = words.map(x => x.toLowerCase())

  // Count up the occurrences of each word
  //  val wordCounts = lowercaseWords.countByValue()

  // Print the results.
  //  wordCounts.foreach(println)

  val wordCounts = lowercaseWords.map(w => (w, 1)).reduceByKey((x, y) => x + y)

  // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
  val wordCountsSorted = wordCounts.map((t) => (t._2, t._1)).sortByKey()

  for (result <- wordCountsSorted.collect()) {
    val count = result._1
    val word = result._2
    println(s"$word: $count")
  }

}