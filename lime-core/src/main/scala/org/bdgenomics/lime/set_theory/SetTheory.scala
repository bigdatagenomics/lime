package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.util.Partitioners.ReferenceRegionRangePartitioner
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

abstract class SetTheory[T: ClassTag] extends Serializable {
  val partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]
  val distanceThreshold: Long

  def primitive(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): ReferenceRegion

  def condition(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): Boolean

}

abstract class SetTheoryBetweenCollections[T: ClassTag, U: ClassTag, RT, RU] extends SetTheory[T] {
  val leftRdd: RDD[(ReferenceRegion, T)]
  val rightRdd: RDD[(ReferenceRegion, U)]

  def compute(): RDD[(ReferenceRegion, (RT, RU))]
}

abstract class SetTheoryWithSingleCollection[T: ClassTag] extends SetTheory[T] {
  val rddToCompute: RDD[(ReferenceRegion, T)]

  def compute(): RDD[(ReferenceRegion, Iterable[T])] = {
    val localComputed = localCompute(rddToCompute.map(f => (f._1, Iterable(f._2))), distanceThreshold)
    externalCompute(localComputed, partitionMap, distanceThreshold, 2)
  }

  protected def localCompute(rdd: RDD[(ReferenceRegion, Iterable[T])], distanceThreshold: Long): RDD[(ReferenceRegion, Iterable[T])] = {
    rdd.mapPartitions(iter => {
      if (iter.hasNext) {
        val currTuple = iter.next()
        var currRegion = currTuple._1
        val tempRegionListBuffer = ListBuffer[(ReferenceRegion, Iterable[T])]()
        val tempValueListBuffer = ListBuffer[T]()
        tempValueListBuffer ++= currTuple._2
        while (iter.hasNext) {
          val tempTuple = iter.next()
          val tempRegion = tempTuple._1
          if (condition(currRegion, tempRegion, distanceThreshold)) {
            currRegion = primitive(currRegion, tempRegion, distanceThreshold)
            tempValueListBuffer ++= currTuple._2
          } else {
            tempRegionListBuffer += ((currRegion, tempValueListBuffer.toIterable))
            currRegion = tempRegion
          }
        }
        tempRegionListBuffer += ((currRegion, tempValueListBuffer.toIterable))
        tempRegionListBuffer.iterator
      } else {
        Iterator()
      }
    })
  }

  /**
    * Computes the primitives between partitions.
    *
    * @param rdd The rdd to compute on.
    * @param partitionMap The current partition map for rdd.
    * @param distanceThreshold The distance threshold for the condition and primitive.
    * @param round The current round of computation in the recursion tree. Increments by a factor of 2 each round.
    * @return The computed rdd for this round.
    */
  @tailrec private def externalCompute(rdd: RDD[(ReferenceRegion, Iterable[T])],
                                partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                distanceThreshold: Long,
                                round: Int): RDD[(ReferenceRegion, Iterable[T])] = {

    if (round > partitionMap.length) {
      return rdd
    }

    lazy val partitionedRdd = rdd.mapPartitionsWithIndex((idx, iter) => {
      val indexWithinParent = idx % round
      val partnerPartition = {
        var i = 1
        if (idx > 0) {
          while (partitionMap(idx - i).isEmpty) {
            i += 1
          }
          idx - i
        } else {
          idx
        }
      }

      val partnerPartitionBounds = partitionMap(partnerPartition)

      iter.map(f => {
        val (region, value) = f
        if (indexWithinParent == round / 2 &&
          (region.covers(partnerPartitionBounds.get._2, distanceThreshold) ||
            region.compareTo(partnerPartitionBounds.get._2) <= 0)) {

          ((region, partnerPartition), value)
        } else {
          ((region, idx), value)
        }
      })
    }).repartitionAndSortWithinPartitions(new ReferenceRegionRangePartitioner(partitionMap.length))
      .map(f => (f._1._1, f._2))


    lazy val newPartitionMap = partitionedRdd.mapPartitions(iter => {
                                 getRegionBoundsFromPartition(iter)
                               }).collect

    externalCompute(localCompute(partitionedRdd, distanceThreshold), newPartitionMap, distanceThreshold, round * 2)
  }

  /**
    * Gets the partition bounds from a ReferenceRegion keyed Iterator
    *
    * @param iter The data on a given partition. ReferenceRegion keyed
    * @return The bounds of the ReferenceRegions on that partition, in an Iterator
    */
  private def getRegionBoundsFromPartition(iter: Iterator[(ReferenceRegion, Iterable[T])]): Iterator[Option[(ReferenceRegion, ReferenceRegion)]] = {
    if (iter.isEmpty) {
      // This means that there is no data on the partition, so we have no bounds
      Iterator(None)
    } else {
      val firstRegion = iter.next
      val lastRegion =
        if (iter.hasNext) {
          // we have to make sure we get the full bounds of this partition, this
          // includes any extremely long regions. we include the firstRegion for
          // the case that the first region is extremely long
          (iter ++ Iterator(firstRegion)).maxBy(f => (f._1.referenceName, f._1.end, f._1.start))
          // only one record on this partition, so this is the extent of the bounds
        } else {
          firstRegion
        }
      Iterator(Some((firstRegion._1, lastRegion._1)))
    }
  }
}
