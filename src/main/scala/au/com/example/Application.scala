package au.com.example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Application extends App with LazyLogging {
  val conf = new SparkConf().setMaster("local").setAppName("My App")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("/usr/local/opt/apache-spark/README.md")
  val pyLines = lines.filter(_.contains("Python"))
  logger.info(s"Number of lines is: ${pyLines.count()}")
}
