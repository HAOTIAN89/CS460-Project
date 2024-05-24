package app.recommender

import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import app.recommender.LSH.{LSHIndex, NNLookup}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext, index: LSHIndex, ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {
  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)


  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val user_movie_ids = ratings.filter(_._1 == userId).map(_._2).collect().toSet
    val genreRDD = sc.parallelize(List(genre))

    // get movies from genre search and filter out movies the user has already rated for being efficient
    val movie_recommend_base = nn_lookup.lookup(genreRDD).flatMap { case (_, movies) => movies }
      .map { case (movie_id, _, _) => movie_id }
      .filter { movie_id => !user_movie_ids.contains(movie_id)}
      .collect().toList
      .map { movie_id => (movie_id, baselinePredictor.predict(userId, movie_id)) }
      .sortBy(-_._2).take(K)

    movie_recommend_base
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val user_movie_ids = ratings.filter(_._1 == userId).map(_._2).collect().toSet
    val genreRDD = sc.parallelize(List(genre))

    val movie_recommend_base = nn_lookup.lookup(genreRDD).flatMap { case (_, movies) => movies }
      .map { case (movie_id, _, _) => movie_id }
      .filter { movie_id => !user_movie_ids.contains(movie_id) }
      .collect().toList
      .map { movie_id => (movie_id, collaborativePredictor.predict(userId, movie_id)) }
      .sortBy(-_._2).take(K)

    movie_recommend_base
  }
}
