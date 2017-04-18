package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.util.Partitioners.ReferenceRegionRangePartitioner
import org.bdgenomics.utils.interval.array.IntervalArray
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

sealed abstract class Complemet[T: ClassTag, U: ClassTag] extends SetTheoryBetweenCollections[T, U, T, U] {

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

    firstRegion.complement(secondRegion, minimumOverlap)
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

case class DistributedComplement[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                             rightRdd: RDD[(ReferenceRegion, U)],
                                                             partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                             threshold: Long = 0L) extends Intersection[T, U] {

  override protected def prepare(): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, U)], Array[Option[(ReferenceRegion, ReferenceRegion)]]) = {
  }

}
