package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val raws = sc.textFile(getClass.getResource(path).getPath)
    // val raw = sc.textFile(path)
    raws.map { raw =>
      val split_parts = raw.split("\\|")
      val movie_id = split_parts(0).toInt
      val movie_name = split_parts(1).replaceAll("\"", "")
      val movie_keywords = split_parts.drop(2).map(_.replaceAll("\"", "")).toList
      (movie_id, movie_name, movie_keywords)
    }
  }
}

