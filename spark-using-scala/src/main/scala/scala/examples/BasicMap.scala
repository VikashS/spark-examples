package scala.examples

import org.apache.spark.SparkContext

object BasicMap {
  def main(args: Array[String]) : Unit = {
    val master = args.length match {
      case x: Int if x>0 => args(0)
      case _ => "local"
    }
    
    val sc = new SparkContext(master,"BasicMap")
    val input = sc.parallelize(List(1,2,3,4,5))
    val result = input.map { x => x*x }
    println(result.collect().mkString(","))
  }
}