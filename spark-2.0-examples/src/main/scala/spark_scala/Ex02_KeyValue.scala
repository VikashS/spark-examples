package spark_scala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Ex02_KeyValue extends App {

  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  //Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Ex02_KeyValue").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  val lines = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/fakefriends.csv")

  // Use our parseLines function to convert to (age, numFriends) tuples
  val rdd = lines.map(parseLine(_))

  // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
  // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
  // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
  // adding together all the numFriends values and 1's respectively.
  val totalByAge = rdd
    .mapValues(numFriend => (numFriend, 1))
    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

  // So now we have tuples of (age, (totalFriends, totalInstances))
  // To compute the average we divide totalFriends / totalInstances for each age.
  val averagesByAge = totalByAge.mapValues(x => x._1 / x._2)

  // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
  val results = averagesByAge.collect()

  // Sort and print the final results.
  results.sorted.foreach(println)

}