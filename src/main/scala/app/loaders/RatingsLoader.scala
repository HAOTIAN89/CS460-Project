package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val raw = sc.textFile(getClass.getResource(path).getPath)
    // val raw = sc.textFile(path)
    raw.map { line =>
      val splitting = line.split("\\|")
      val user_id = splitting(0).toInt
      val movie_id = splitting(1).toInt
      val old_rating = if (splitting.length > 4 && splitting(2).nonEmpty) Some(splitting(2).toDouble) else None // option
      val new_rating = splitting(if (splitting.length == 4) 2 else 3).toDouble
      val timestamp = splitting(if (splitting.length == 4) 3 else 4).toInt
      (user_id, movie_id, old_rating, new_rating, timestamp)
     }
  }
}