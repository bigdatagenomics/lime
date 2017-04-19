package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

sealed abstract class Intersection[T: ClassTag, U: ClassTag] extends OverlapBasedSetTheory[T, U, T, U] {

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
      condition(rightRegion, currentLeftRegion, threshold)
    }).map(g => {
      val (currentRightRegion, currentRightValue) = g
      (primitive(currentLeftRegion, currentRightRegion), (currentLeftValue, currentRightValue))
    }).iterator
  }

}
