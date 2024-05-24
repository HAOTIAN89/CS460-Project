package app.recommender.collaborativeFiltering

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD


class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // convert ratingsRDD to a format suitable for ALS
    val train_data = ratingsRDD.map {
      case (user_id, movie_id, _, new_rating, _) => Rating(user_id, movie_id, new_rating)
    }

    // train the model
    model = ALS.train(ratings = train_data,
      rank = rank,
      iterations = maxIterations,
      lambda = regularizationParameter,
      blocks = n_parallel,
      seed = seed)
  }

  def predict(userId: Int, movieId: Int): Double = {
    val predict_result = model.predict(userId, movieId)
    
    predict_result
  }

}
