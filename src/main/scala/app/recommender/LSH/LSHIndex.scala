package app.recommender.LSH

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {
  private val minhash = new MinHash(seed)
  private val hashes = hash(data.map(_._3))

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets()
  : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    val buckets = data.zip(hashes).map {
      case ((id, name, keywords), (signature, _)) =>
        (signature, (id, name, keywords))
    }.groupByKey()
    .mapValues(_.toList)
    .partitionBy(new HashPartitioner(data.partitions.length))

    // optional
    buckets.cache()
    buckets
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    val buckets = getBuckets()
    val queries_matched = queries.leftOuterJoin(buckets).map {
      case (signature, (payload, result)) =>
        (signature, payload, result.getOrElse(List.empty))
    }

    queries_matched
  }
}
