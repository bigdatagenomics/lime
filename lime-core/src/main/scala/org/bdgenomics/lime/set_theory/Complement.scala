package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.reflect.ClassTag

sealed abstract class Complement[T: ClassTag] extends SetTheoryWithSingleCollection[T] {

  val referenceNameBounds: Map[String, ReferenceRegion]

  def primitive(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): ReferenceRegion = {

    firstRegion.merge(secondRegion, distanceThreshold)
  }

  def condition(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): Boolean = {

    firstRegion.overlaps(secondRegion, distanceThreshold)
  }

  /**
   * Post processing in Complement actually performs the complement on the
   * merged regions. On the last node, regions missing from the input are
   * added.
   *
   * @param rdd The RDD after computation is complete.
   * @return The RDD after post-processing.
   */
  protected def postProcess(rdd: RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])]): RDD[(ReferenceRegion, Iterable[T])] = {
    val existingComplements = getComplement(rdd, partitionMap)
    existingComplements.cache()
    val distinctRegionsCovered = existingComplements.map(_._1.referenceName).distinct().collect()
    existingComplements.mapPartitionsWithIndex((idx, iter) => {
      if (idx == partitionMap.length - 1) {
        iter.map(f => (f._1, f._2.map(_._2))) ++ (referenceNameBounds.keys.toSet -- distinctRegionsCovered).toList.sorted
          .map(f => (referenceNameBounds(f), Iterable.empty[T]))
      } else {
        iter.map(f => (f._1, f._2.map(_._2)))
      }
    })
  }

  protected def getComplement(locallyMerged: RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])],
                              partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]): RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])] = {

    val sortedReferenceNames = referenceNameBounds.keys.toSeq.sorted
    locallyMerged.mapPartitionsWithIndex((idx, iter) => {
      if (iter.nonEmpty) {
        val next = iter.next
        val first =
          if (idx == 0) {
            (next._1, (ReferenceRegion(next._1.referenceName, 0, next._1.start), next._2))
          } else {
            val lastBoundOnPreviousPartition = partitionMap.apply(idx - 1).get._2
            (next._1, (ReferenceRegion(next._1.referenceName, lastBoundOnPreviousPartition.end, next._1.start, next._1.strand), next._2))
          }
        val localComplement = iter.foldLeft(List(first))((b, a) => {
          if (b.head._1.referenceName == a._1.referenceName) {
            (a._1, (ReferenceRegion(b.head._1.referenceName, b.head._1.end, a._1.start, b.head._1.strand), a._2)) :: b
          } else {
            val indexOfNext = sortedReferenceNames.indexOf(a._1.referenceName)
            val indexDifference = indexOfNext - sortedReferenceNames.indexOf(b.head._1.referenceName)
            if (indexDifference != 1) {
              val x = for (i <- 1 until indexDifference) yield {
                (referenceNameBounds(sortedReferenceNames(indexOfNext + i)), (referenceNameBounds(sortedReferenceNames(indexOfNext + i)), a._2))
              }
              (a._1, (ReferenceRegion(a._1.referenceName, 0, a._1.start, a._1.strand), a._2)) :: x.toList ++
                List((b.head._1, (ReferenceRegion(b.head._1.referenceName, b.head._1.end, referenceNameBounds(b.head._1.referenceName).end, b.head._1.strand), b.head._2._2))).++(b)
            } else {
              List((a._1, (ReferenceRegion(a._1.referenceName, 0, a._1.start, a._1.strand), a._2)),
                (b.head._1, (ReferenceRegion(b.head._1.referenceName, b.head._1.end, referenceNameBounds(b.head._1.referenceName).end, b.head._1.strand), b.head._2._2))).++(b)
            }
          }
        })
        val interNodeEnds =
          if (idx < partitionMap.length - 1 &&
            !partitionMap(idx).exists(_._1.referenceName != localComplement.head._1.referenceName)) {
            List((localComplement.head._1,
              (ReferenceRegion(localComplement.head._1.referenceName,
                localComplement.head._1.end,
                referenceNameBounds(localComplement.head._1.referenceName).end),
                localComplement.head._2._2)))
          } else {
            List()
          }
        (interNodeEnds ++ localComplement).map(f => f._2).reverseIterator
      } else {
        Iterator()
      }
    })
  }
}

case class DistributedComplement[T: ClassTag](rddToCompute: RDD[(ReferenceRegion, T)],
                                              partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                              referenceNameBounds: Map[String, ReferenceRegion],
                                              threshold: Long = 0L) extends Complement[T]