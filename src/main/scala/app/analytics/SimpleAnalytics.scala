package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  private var titlesGroupedById: RDD[(Int, Iterable[(Int, String, List[String])])] = null
  private var ratingsGroupedByYearByTitle: RDD[(Int, Map[Int, Iterable[(Int, Int, Option[Double], Double, Int)]])] = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movies: RDD[(Int, String, List[String])]
          ): Unit = {
    val numPartitions = 5 //hyper-parameter
    ratingsPartitioner = new HashPartitioner(numPartitions)
    moviesPartitioner = new HashPartitioner(numPartitions)

    val ratings_by_year = ratings.map{rating =>
      val year = DateTime(rating._5 * 1000L).getYear
      (rating._1, rating._2, rating._3, rating._4, year)
    }.groupBy(_._5)

    val ratings_by_year_by_movie_id = ratings_by_year.mapValues(ratings => ratings.groupBy(_._2))
    val movies_by_id = movies.groupBy(_._1)

    ratingsGroupedByYearByTitle = ratings_by_year_by_movie_id.partitionBy(ratingsPartitioner).persist(MEMORY_AND_DISK)
    titlesGroupedById = movies_by_id.partitionBy(moviesPartitioner).persist(MEMORY_AND_DISK)
  }

  // helper function
  private def computeMostRatedMovieIdByYear: RDD[(Int, Int)] = {
    val movieRatingsCountByYear: RDD[(Int, Map[Int, Int])] = ratingsGroupedByYearByTitle.mapValues { movie_ratings =>
      movie_ratings.map { case (movie_id, ratings) =>
        (movie_id, ratings.size)
      }
    }

    movieRatingsCountByYear.mapValues { movie_counts =>
      movie_counts.toList.sortWith{
        (left, right) =>
          if (left._2 == right._2) left._1 > right._1 // sort by movie ID in descending if counts are the same
          else left._2 > right._2 // otherwise sort by count in descending
      }.head._1
    }.map {
      case (year, movie_id) => (movie_id, year)
    }
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    val number_Of_Movies = ratingsGroupedByYearByTitle.map{case(year, movie_ratings) => (year, movie_ratings.keys.size)}

    number_Of_Movies
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    // compute the mostRatedMovieIdByYear
    val mostRatedMovieIdByYear = computeMostRatedMovieIdByYear

    // join with titles to get the movie name
    val mostRatedMovieNameByYear: RDD[(Int, String)] = mostRatedMovieIdByYear
      .join(titlesGroupedById.mapValues(_.head))
      .map { case (_, (year, (_, movie_name, _))) => (year, movie_name) }

    mostRatedMovieNameByYear
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    // compute the mostRatedMovieIdByYear
    val mostRatedMovieIdByYear = computeMostRatedMovieIdByYear

    // map movies IDs to their genres, extracting genres for the most rated movies
    val mostRatedMovieGenresByYear: RDD[(Int, List[String])] = mostRatedMovieIdByYear
      .join(titlesGroupedById.mapValues(_.head))
      .map { case (_, (year, (_, _, movie_genre))) => (year, movie_genre) }

    mostRatedMovieGenresByYear
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getLeastAndMostRatedGenreAllTime: ((String, Int), (String, Int)) = {
    val genres_most_rated = getMostRatedGenreEachYear
    val genre_counts = genres_most_rated.flatMap {
      case (_, genres) => genres.map(genre => (genre, 1))
    }
    val total_genre_counts = genre_counts.reduceByKey(_ + _)
    val most_genre = total_genre_counts
      .sortBy(x=>(-x._2, x._1)).first()
    val least_genre = total_genre_counts
      .sortBy(x => (x._2, x._1)).first()

    (least_genre, most_genre)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    val required_genres = requiredGenres.collect().toSet

    val filtered_movies = movies.filter {
      case (_, _, genres) => genres.exists(required_genres.contains)
    }.map(_._2)

    filtered_movies
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    val broadcast_genres = broadcastCallback(requiredGenres)
    val filtered_movies_with_broadcast = movies.filter {
      case (_, _, genres) => genres.exists(broadcast_genres.value.contains)
    }.map(_._2)

    filtered_movies_with_broadcast
  }

}

