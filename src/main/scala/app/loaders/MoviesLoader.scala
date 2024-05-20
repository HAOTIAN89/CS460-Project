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
    val raw = sc.textFile(getClass.getResource(path).getPath)
    // val raw = sc.textFile(path)
    raw.map { line =>
      val splitting = line.split("\\|")
      val movie_id = splitting(0).toInt
      val movie_name = splitting(1).replaceAll("\"", "")
      val movie_keywords = splitting.drop(2).map(_.replaceAll("\"", "")).toList
      (movie_id, movie_name, movie_keywords)
    }
  }
}

