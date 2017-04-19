package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.util.Partitioners.ReferenceRegionRangePartitioner
import org.bdgenomics.utils.interval.array.IntervalArray

import scala.reflect.ClassTag

/**
 * Created by DevinPetersohn on 4/19/17.
 */
abstract class OverlapBasedSetTheory[T: ClassTag, U: ClassTag, RT, RU] extends SetTheoryBetweenCollections[T, U, RT, RU] {

  /**
   * Flags regions that no longer overlap with the query region to be removed
   * from the cache
   *
   * @see pruneCache
   * @param cachedRegion The current region in the cache.
   * @param to The region that is compared against.
   * @return True for regions that should be removed.
   *         False for all regions that should remain in the cache.
   */
  override protected def pruneCacheCondition(cachedRegion: ReferenceRegion,
                                             to: ReferenceRegion): Boolean = {
    cachedRegion.compareTo(to) < 0 && !cachedRegion.covers(to)
  }

  /**
   * Flags regions that overlap the query to be added to the cache.
   *
   * @see advanceCache
   * @param candidateRegion The current candidate region.
   * @param until The region to compare against.
   * @return True for all regions to be added to the cache.
   *         False for regions that should not be added to the cache.
   */
  override protected def advanceCacheCondition(candidateRegion: ReferenceRegion,
                                               until: ReferenceRegion): Boolean = {
    candidateRegion.compareTo(until) <= 0 || candidateRegion.covers(until)
  }

  /**
   * Prepares the RDDs for the overlap based primitive operation.
   *
   * @return The prepared left RDD, the prepared right RDD, the partition map
   */
  override protected def prepare(): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, U)], Array[Option[(ReferenceRegion, ReferenceRegion)]]) = {

    val adjustedPartitionMapWithIndex =
      // the zipWithIndex gives us the destination partition ID
      partitionMap.map(_.get).zipWithIndex.map(g => {
        // in the case where we span multiple referenceNames
        if (g._1._1.referenceName != g._1._2.referenceName) {
          // create a ReferenceRegion that goes to the end of the chromosome
          (ReferenceRegion(
            g._1._1.referenceName,
            g._1._1.start,
            g._1._1.end),
            g._2)
        } else {
          // otherwise we just have the ReferenceRegion span from partition
          // start to end
          (ReferenceRegion(
            g._1._1.referenceName,
            g._1._1.start,
            g._1._2.end),
            g._2)
        }
      })

    val partitionMapIntervals = IntervalArray(
      adjustedPartitionMapWithIndex,
      adjustedPartitionMapWithIndex.maxBy(_._1.width)._1.width,
      sorted = true)

    val preparedRightRdd =
      rightRdd.mapPartitions(iter => {
        iter.flatMap(f => {
          val rangeOfHits = partitionMapIntervals.get(f._1, requireOverlap = false)
          rangeOfHits.map(g => ((f._1, g._2), f._2))
        })
      }, preservesPartitioning = true)
        .repartitionAndSortWithinPartitions(
          new ReferenceRegionRangePartitioner(partitionMap.length))
        // return to an RDD[(ReferenceRegion, T)], removing the partition ID
        .map(f => (f._1._1, f._2))
    (leftRdd, preparedRightRdd, partitionMap)
  }
}
