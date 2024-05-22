package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = null

  // movie_id_ratings: (id, (name, avg_rating, count, genre))
  private var movie_id_ratings: RDD[(Int, (String, Double, Int, List[String]))] = null
  private val pattern = (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *                format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    val numPartitions = ratings.getNumPartitions
    partitioner = new HashPartitioner(numPartitions)

    // aggregate ratings by id
    // first we need to design the aggregate pattern
    val zeroValue = (0.0, 0)
    val seqOp = pattern
    val combOp = pattern

    val ratings_aggregated = ratings.map {
      case (_, id, _, rating, _) => (id, (rating, 1))
    }.aggregateByKey(zeroValue)(seqOp, combOp).mapValues {
      case (sum, count) => if (count > 0) (sum / count, count) else (0.0, 0)
    }

    // combine rating aggregates with id, handling unrated id
    val titles_grouped = title.groupBy(_._1).mapValues(_.map(t => (t._2, t._3)))
    val movie_id_ratings_aggregated = ratings_aggregated
      .fullOuterJoin(titles_grouped)
      .flatMap {
        case (id, (ratingOption, titleOption)) =>
          titleOption.getOrElse(Seq()).map {
            case (name, keywords) =>
              (id, ratingOption.map(_._1).getOrElse(0.0), ratingOption.map(_._2).getOrElse(0), name, keywords)
          }
      }
      .map { case (id, avg_rating, count, name, keywords) =>
        (id, (name, avg_rating, count, keywords))
      }

    // partitioning and persistence
    movie_id_ratings = movie_id_ratings_aggregated.partitionBy(partitioner).persist(MEMORY_AND_DISK)
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    val result = movie_id_ratings.map { case (_, (name, avg_rating, _, _)) => (name, avg_rating) }

    result
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    // first we still need to design the aggregate pattern
    val zeroValue = (0.0, 0)
    val seqOp = (acc: (Double, Int), elem: (Int, (String, Double, Int, List[String]))) => {
      val (total_rating, total_count) = acc
      val (_, (name, avg_rating, count, _)) = elem
      if (count > 0) (total_rating + avg_rating, total_count + 1) else acc
    }
    val combOp = pattern

    // filter movies by keywords
    val filtered_titles = movie_id_ratings.filter {
      case (_, (_, _, _, id_keywords)) =>
        keywords.forall(id_keywords.contains)
    }

    // aggregate to compute the total rating and count
    val (total_rating, total_count) = filtered_titles.aggregate(zeroValue)(seqOp, combOp)

    // calculate and return the final result
    val kq_result = if (total_count == 0) {
      if (filtered_titles.isEmpty()) -1.0 // no id exist with the given keywords
      else 0.0 // ids exist but are not rated
    } else {
      total_rating / total_count
    }

    kq_result
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   *              format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // convert the array of updates into an RDD
    val deltaRDD = sc.parallelize(delta_)

    // then design the aggregate pattern
    val zeroValue = (0.0, 0, 0.0)
    val seqOp = (acc: (Double, Int, Double), value: (Option[Double], Double)) => {
      val (sum_ratings, num_ratings, adjustment) = acc
      val (old_rating, new_rating) = value
      old_rating match {
        case Some(old) => (sum_ratings + new_rating - old, num_ratings, adjustment + new_rating - old)
        case None => (sum_ratings + new_rating, num_ratings + 1, adjustment + new_rating)
      }
    }
    val combOp = (acc1: (Double, Int, Double), acc2: (Double, Int, Double)) => {
      (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
    }

    // aggregate changes by id
    val update_aggregated = deltaRDD.map{
      case (_, id, old_rating, new_rating, _) =>
        (id, (old_rating, new_rating))
    }.aggregateByKey(zeroValue)(seqOp, combOp)

    // join the aggregated updates with the existing data
    val updated_ratings = movie_id_ratings.leftOuterJoin(update_aggregated).map {
      case (id, ((name, avg_rating, count, keywords), Some((sum_ratings, num_ratings, adjustment)))) =>
        val new_count = count + num_ratings
        val new_sum = avg_rating * count + adjustment
        val updated_avg = if (new_count > 0) new_sum / new_count else 0.0
        (id, (name, updated_avg, new_count, keywords))
      case (id, (details, None)) =>
        (id, details) // no updates for this movie
    }

    // un persist the old RDD and persist the new one
    movie_id_ratings.unpersist()
    movie_id_ratings = updated_ratings.partitionBy(partitioner).persist(MEMORY_AND_DISK)
  }
}
