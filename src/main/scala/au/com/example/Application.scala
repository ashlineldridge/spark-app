package au.com.example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}

object Application extends App with LazyLogging {

  parseArgs match {
    case (Some(inputFile), Some(outputFile)) =>
      val conf = new SparkConf().setMaster("local").setAppName("WordCount")
      val sc = new SparkContext(conf)
      val lines = sc.textFile(inputFile)
      val words = lines.flatMap(_.split("\\s"))
      val counts = words.map(w => (w, 1)).reduceByKey { case (x, y) => x + y }
      counts.saveAsTextFile(outputFile)
      logger.info("Done")
    case _ =>
      logger.error("Error: Missing arguments")
  }

  def parseArgs: (Option[String], Option[String]) = args.toList match {
    case h :: t => (Some(h), t.headOption)
    case _      => (None, None)
  }
}
