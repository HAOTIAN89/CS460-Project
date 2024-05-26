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
    val raws = sc.textFile(getClass.getResource(path).getPath)
    // val raw = sc.textFile(path)
    raws.map { raw =>
      val split_parts = raw.split("\\|")
      val user_id = split_parts(0).toInt
      val movie_id = split_parts(1).toInt
      val old_rating = if (split_parts.length > 4 && split_parts(2).nonEmpty)
        Some(split_parts(2).toDouble) else None // option
      val new_rating = split_parts(if (split_parts.length == 4) 2 else 3).toDouble
      val timestamp = split_parts(if (split_parts.length == 4) 3 else 4).toInt
      (user_id, movie_id, old_rating, new_rating, timestamp)
     }
  }
}