package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

sealed abstract class Subtract[T: ClassTag, U: ClassTag] extends OverlapBasedSetTheory[T, U, T, Option[U]] {
  /**
   * The condition that should be met in order for the primitive to be
   * computed.
   *
   * @param firstRegion       The region to test against.
   * @param secondRegion      The region to test.
   * @param distanceThreshold The threshold for the primitive.
   * @return True if the threshold requirement is met.
   *         False if the threshold requirement is not met.
   */
  override protected def condition(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   distanceThreshold: Long = 0L): Boolean = {

    firstRegion.overlapsBy(secondRegion).exists(_ >= distanceThreshold)
  }

  override protected def primitive(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   distanceThreshold: Long = 0L): ReferenceRegion = {

    ReferenceRegion(firstRegion.referenceName,
      secondRegion.end,
      firstRegion.end,
      firstRegion.strand)
  }

  /**
   * The primitive operation for subtraction, computes the primitive of the
   * two regions.
   *
   * @param firstRegion       The first region for the primitive.
   * @param secondRegion      The second region for the primitive.
   * @param distanceThreshold The threshold for the primitive.
   * @return The computed primitive for the two regions.
   */
  protected def subtract(firstRegion: ReferenceRegion,
                         secondRegion: ReferenceRegion,
                         distanceThreshold: Long = 0L): Iterable[ReferenceRegion] = {
    val first = if (secondRegion.start > firstRegion.start) {
      Iterable(ReferenceRegion(firstRegion.referenceName,
        firstRegion.start,
        secondRegion.start,
        firstRegion.strand))
    } else {
      Iterable()
    }
    val second = if (firstRegion.end > secondRegion.end) {
      Iterable(ReferenceRegion(firstRegion.referenceName,
        secondRegion.end,
        firstRegion.end,
        firstRegion.strand))
    } else {
      Iterable()
    }

    first ++ second
  }
}

case class DistributedSubtract[T: ClassTag, U: ClassTag](protected val leftRdd: RDD[(ReferenceRegion, T)],
                                                         protected val rightRdd: RDD[(ReferenceRegion, U)],
                                                         protected val partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                         protected val threshold: Long = 0L) extends Subtract[T, U] {

  /**
   * Processes all hits from the cache and creates an iterator for the current
   * left based on the primitive operation.
   *
   * @param current The current left row, keyed by the ReferenceRegion.
   * @param cache   The cache of potential hits.
   * @return An iterator containing all processed hits.
   */
  override protected def processHits(current: (ReferenceRegion, T),
                                     cache: ListBuffer[(ReferenceRegion, U)]): Iterator[(ReferenceRegion, (T, Option[U]))] = {

    val (currentLeftRegion, currentLeftValue) = current
    val filteredCache = cache.filter(f => {
      val (rightRegion, _) = f
      condition(currentLeftRegion, rightRegion, threshold)
    })

    if (filteredCache.isEmpty) {
      Iterator((currentLeftRegion, (currentLeftValue, None)))
    } else {
      filteredCache.foldLeft(List(filteredCache.head))((b, a) => {
        if (b.head._1.overlaps(a._1)) {
          b.drop(1).+:((b.head._1.merge(a._1), b.head._2))
        } else {
          b.+:(a)
        }
      }).flatMap(g => {
        val (currentRightRegion, currentRightValue) = g
        subtract(currentLeftRegion, currentRightRegion, threshold).map(region => {
          (region, (currentLeftValue, Some(currentRightValue)))
        })
      }).toIterator
    }
  }
}