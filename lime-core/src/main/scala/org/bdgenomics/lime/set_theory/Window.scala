package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

sealed abstract class Window[T: ClassTag, U: ClassTag] extends OverlapBasedSetTheory[T, U, T, U] {
  /**
   * The primitive operation for Window is a no-op.
   *
   * @param firstRegion The first region to window.
   * @param secondRegion The second region to window.
   * @param maximumDistance The maximum distance allowed.
   * @return The firstRegion.
   */
  override protected def primitive(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   maximumDistance: Long = 1000L): ReferenceRegion = {

    firstRegion
  }

  /**
   * The condition that should be met in order for the two regions to be
   * windowed.
   *
   * @param firstRegion The region to test against.
   * @param secondRegion The region to test.
   * @param maximumDistance The maximum distance allowed.
   * @return True if the distance requirement is met.
   *         False if the distance requirement is not met.
   */
  override protected def condition(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   maximumDistance: Long = 1000L): Boolean = {

    firstRegion.isNearby(secondRegion, maximumDistance)
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
    cachedRegion.compareTo(to) < 0 && !to.isNearby(cachedRegion, threshold)
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
    candidateRegion.compareTo(until) <= 0 || until.isNearby(candidateRegion, threshold)
  }
}

case class DistributedWindow[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                       rightRdd: RDD[(ReferenceRegion, U)],
                                                       partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                       threshold: Long = 1000L) extends Window[T, U] {
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
      condition(currentLeftRegion, rightRegion, threshold)
    }).map(g => {
      val (currentRightRegion, currentRightValue) = g
      (primitive(currentLeftRegion, currentRightRegion, threshold), (currentLeftValue, currentRightValue))
    }).iterator
  }
}