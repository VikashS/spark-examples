package examples

import org.apache.spark.sql.SparkSession

object RDDToDataSet {
  def main(args: Array[String]) : Unit = {
    val sparkSession = SparkSession.builder().appName("example").master("local").getOrCreate()
    
    val sparkContext = sparkSession.sparkContext
    
    import sparkSession.implicits._
    
    //read data from text file
    val rdd = sparkContext.textFile("src/main/resources/data.txt")
    val ds = sparkSession.read.text("src/main/resources/data.txt").as[String]
    
    // do count
    println("count ")
    println(rdd.count())
    println(ds.count())
    
    // wordcount
    println(" wordcount ")
    
    val wordsRDD = rdd.flatMap { x => x.split("\\s+") }
    val wordsPair = wordsRDD.map { x => (x,1) }
    val wordCount = wordsPair.reduceByKey(_+_)
    println(wordCount.collect.toList)
    
    val wordsDs = ds.flatMap(_.split("\\s+"))
    val wordsPairDs = wordsDs.groupByKey(v=>v)
    val wordsCountDs = wordsPairDs.count()
    wordsCountDs.show()
    
    rdd.cache()
    ds.cache()
    
    //map partitions
    val mapPartitionsRDD = rdd.mapPartitions(itr=>List(itr.count { x => true }).iterator)
    println(s" the count each partition is ${mapPartitionsRDD.collect().toList}")
    
    val mapPartitionsDs = ds.mapPartitions(iterator => List(iterator.count(value => true)).iterator)
    mapPartitionsDs.show()
    
    //reduceByKey API
    val reduceCountByRDD = wordsPair.reduceByKey(_+_)
    val reduceCountByDs = wordsPairDs.mapGroups((k,v)=>(k,v.length))
    println(reduceCountByRDD.collect().toList)
    println(reduceCountByDs.collect().toList)
    
    
  }
}