package app.recommender.baseline

import org.apache.spark.rdd.RDD


class BaselinePredictor() extends Serializable {
  private var state = null
  private var user_avg_ratings: RDD[(Int, Double)] = null
  private var movie_avg_deviations: RDD[(Int, Double)] = null
  private var global_avg: Double = 0.0

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // compute the average ratings per user
    user_avg_ratings = ratingsRDD.map {
      case (user_id, _, _, new_ratings, _) => (user_id, (new_ratings, 1))
    }.reduceByKey {
      case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
    }.mapValues{
      case (sum, count) => sum / count
    }

    // compute the average deviations per movie
    movie_avg_deviations = ratingsRDD.map {
      case (user_id, movie_id, _, new_rating, _) => (user_id, (movie_id, new_rating))
    }.join(user_avg_ratings).map{
      case (user_id, ((movie_id, new_ratings), user_avg_rating)) =>
        val deviation = calculateDeviation(new_ratings, user_avg_rating)
        (movie_id, (deviation, 1))
    }.reduceByKey {
      case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
    }.mapValues {
      case (sum, count) => sum / count
    }

    // compute the global average rating
    global_avg = ratingsRDD.map(_._4).mean()
  }

  private def calculateDeviation(rating: Double, userAvg: Double): Double = {
    if (rating > userAvg) (rating - userAvg) / (5 - userAvg)
    else if (rating < userAvg) (rating - userAvg) / (userAvg - 1)
    else 0.0
  }

  def predict(userId: Int, movieId: Int): Double = {
    val user_avg = user_avg_ratings.lookup(userId).headOption.getOrElse(global_avg)
    val movies_avg_deviation = movie_avg_deviations.lookup(movieId).headOption.getOrElse(0.0)

    val predict_result = if (movies_avg_deviation == 0.0) user_avg
    else {
      val adjustment = if (user_avg + movies_avg_deviation > user_avg) (5 - user_avg)
      else if (user_avg + movies_avg_deviation < user_avg) (user_avg - 1)
      else 1
      user_avg + movies_avg_deviation * adjustment
    }

    predict_result
  }
}
