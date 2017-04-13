package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.util.Partitioners.ReferenceRegionRangePartitioner
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

abstract class SetTheory[T: ClassTag] extends Serializable {
  val partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]
  val distanceThreshold: Long

  def primitive(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): ReferenceRegion

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
    externalCompute(localComputed, distanceThreshold, 2)
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
          if (currRegion.isNearby(tempRegion, distanceThreshold)) {
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

  protected def externalCompute(rdd: RDD[(ReferenceRegion, Iterable[T])],
                                distanceThreshold: Long, round: Int): RDD[(ReferenceRegion, Iterable[T])] = {

    if (round == partitionMap.length) {
      return rdd
    }

    val partitionedRdd = rdd.mapPartitionsWithIndex((idx, iter) => {
      val indexWithinParent = idx % round
      val partnerPartitionBounds =
        if (idx > 0) {
          partitionMap(idx - 1).get
        } else {
          partitionMap(idx).get
        }

      iter.map(f => {
        val (region, value) = f
        if (indexWithinParent == round / 2 &&
          (region.covers(partnerPartitionBounds._2, distanceThreshold) ||
            region.compareTo(partnerPartitionBounds._2) <= 0)) {

          ((region, idx - 1), value)
        } else {
          ((region, idx), value)
        }
      })
    }).repartitionAndSortWithinPartitions(new ReferenceRegionRangePartitioner(partitionMap.length))
      .map(f => (f._1._1, f._2))

    externalCompute(localCompute(partitionedRdd, distanceThreshold), distanceThreshold, round * 2)
  }
}
