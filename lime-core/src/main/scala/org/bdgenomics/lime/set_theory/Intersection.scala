package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

sealed abstract class Intersection[T: ClassTag, U: ClassTag] extends SetTheoryBetweenCollections[T, U, T, U] {

  val threshold: Long

  def primitive(currRegion: ReferenceRegion,
                tempRegion: ReferenceRegion,
                minimumOverlap: Long = 0L): ReferenceRegion = {

    currRegion.intersection(tempRegion, minimumOverlap)
  }

  def condition(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                minimumOverlap: Long = 0L): Boolean = {

    firstRegion.overlapsBy(secondRegion).exists(_ >= threshold)
  }
}

case class DistributedIntersection[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                             rightRdd: RDD[(ReferenceRegion, U)],
                                                             partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                             threshold: Long = 0L) extends Intersection[T, U] {

  private val cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty[(ReferenceRegion, U)]

  def compute(): RDD[(ReferenceRegion, (T, U))] = {
    leftRdd.zipPartitions(rightRdd)(sweep)
  }

  private def sweep(leftIter: Iterator[(ReferenceRegion, T)],
                    rightIter: Iterator[(ReferenceRegion, U)]): Iterator[(ReferenceRegion, (T, U))] = {

    makeIterator(leftIter.buffered, rightIter.buffered)
  }

  private def makeIterator(left: BufferedIterator[(ReferenceRegion, T)],
                           right: BufferedIterator[(ReferenceRegion, U)]): Iterator[(ReferenceRegion, (T, U))] = {

    def advanceCache(until: ReferenceRegion) = {
      while (right.hasNext && (right.head._1.compareTo(until) <= 0 ||
        right.head._1.covers(until))) {

        cache += right.next
      }
    }

    def pruneCache(to: ReferenceRegion) = {
      cache.trimStart({
        val index = cache.indexWhere(f => !(f._1.compareTo(to) < 0 && !f._1.covers(to)))
        if (index <= 0) {
          0
        } else {
          index
        }
      })
    }

    left.flatMap(f => {
      val (currentRegion, _) = f
      advanceCache(currentRegion)
      pruneCache(currentRegion)
      processHits(f)
    })
  }

  private def processHits(current: (ReferenceRegion, T)): Iterator[(ReferenceRegion, (T, U))] = {

    val (currentRegion, _) = current
    cache.filter(f => f._1.overlapsBy(currentRegion).exists(_ >= threshold))
      .map(g => {
        (currentRegion.intersection(g._1, threshold), (current._2, g._2))
      }).iterator
  }

}
