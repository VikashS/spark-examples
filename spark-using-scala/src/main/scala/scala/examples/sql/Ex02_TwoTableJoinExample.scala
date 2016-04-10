package scala.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.reflect.runtime.universe

object Ex02_TwoTableJoinExample {

  case class Order(OrderID: String, CustomerID: String, EmployeeID: String, OrderDate: String, ShipCountry: String)
  
  case class OrderDetails(OrderID: String, ProductID: String, UnitPrice: Float, Qty: Int, Discount: Float)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SQLSimpleSelect")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val ordersFile = sc.textFile("data/NW-Orders-NoHdr.csv")
    println("Orders File has %d Lines.".format(ordersFile.count()))
    val orders = ordersFile.map(_.split(",")).map(e => Order(e(0), e(1), e(2), e(3), e(4)))
    println(orders.count)
    orders.toDF().registerTempTable("Orders")
    var result = sqlContext.sql("SELECT * from Orders")
    result.take(10).foreach(println)

    val orderDetFile = sc.textFile("data/NW-Order-Details-NoHdr.csv")
    println("Order Details File has %d Lines.".format(orderDetFile.count()))
    val orderDetails = orderDetFile.map(_.split(",")).map(e => OrderDetails(e(0), e(1), e(2).trim.toFloat, e(3).trim.toInt, e(4).trim.toFloat))
    println(orderDetails.count)
    orderDetails.toDF().registerTempTable("OrderDetails")
    result = sqlContext.sql("SELECT * from OrderDetails")
    result.take(10).foreach(println)

    result = sqlContext.sql("SELECT OrderDetails.OrderID,ShipCountry,UnitPrice,Qty,Discount FROM Orders INNER JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID")
    result.take(10).foreach(println)
    result.take(10).foreach(e => println("%s | %15s | %5.2f | %d | %5.2f |".format(e(0), e(1), e(2), e(3), e(4))))

    //
    // Sales By Country
    //
//    result = sqlContext.sql("SELECT ShipCountry, Sum(OrderDetails.UnitPrice * Qty * Discount) AS ProductSales FROM Orders INNER JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID GROUP BY ShipCountry")
//    result.take(10).foreach(println)
//     Need to try this
//     println(result.take(30).mkString(" | "))
//     probably this would work
//    result.take(30).foreach(e => println("%15s | %9.2f |".format(e(0), e(1))))
  }
}