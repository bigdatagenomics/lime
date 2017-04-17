package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.util.Partitioners.ReferenceRegionRangePartitioner
import org.bdgenomics.utils.interval.array.IntervalArray
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

sealed abstract class Intersection[T: ClassTag, U: ClassTag] extends SetTheoryBetweenCollections[T, U, T, U] {

  /**
   * The primitive operation for intersection, computes the intersection
   * of the two regions.
   *
   * @param firstRegion The first region to intersect.
   * @param secondRegion The second region to intersect.
   * @param minimumOverlap The minimum amount of overlap required to intersect.
   * @return The intersection of the two regions.
   */
  override protected def primitive(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   minimumOverlap: Long = 0L): ReferenceRegion = {

    firstRegion.intersection(secondRegion, minimumOverlap)
  }

  /**
   * The condition that should be met in order for the two regions to be
   * intersected.
   *
   * @param firstRegion The region to test against.
   * @param secondRegion The region to test.
   * @param minimumOverlap The minimum amount of overlap between the two
   *                       regions.
   * @return True if the overlap requirement is met.
   *         False if the overlap requirement is not met.
   */
  override protected def condition(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   minimumOverlap: Long = 0L): Boolean = {

    firstRegion.overlapsBy(secondRegion).exists(_ >= threshold)
  }
}

case class DistributedIntersection[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                             rightRdd: RDD[(ReferenceRegion, U)],
                                                             partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                             threshold: Long = 0L) extends Intersection[T, U] {

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
   * Pairs the rows together for each intersected region.
   *
   * @see condition
   * @see primitive
   * @param current The current left row, keyed by the ReferenceRegion.
   * @param cache The cache of potential hits.
   * @return An iterator containing all processed hits.
   */
  override protected def processHits(current: (ReferenceRegion, T),
                                     cache: ListBuffer[(ReferenceRegion, U)]): Iterator[(ReferenceRegion, (T, U))] = {

    val (currentLeftRegion, currentLeftValue) = current
    cache.filter(f => {
      val (rightRegion, _) = f
      condition(rightRegion, currentLeftRegion)
    }).map(g => {
      val (currentRightRegion, currentRightValue) = g
      (primitive(currentLeftRegion, currentRightRegion), (currentLeftValue, currentRightValue))
    }).iterator
  }

}
