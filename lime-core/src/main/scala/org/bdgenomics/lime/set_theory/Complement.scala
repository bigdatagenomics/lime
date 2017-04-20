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
    // cache here to avoid recomputing
    existingComplements.cache()
    val distinctReferenceNamesCovered = existingComplements.map(_._1.referenceName).distinct().collect()
    // on the last partition, append the reference names that we did not have previously
    val allComplements = existingComplements.mapPartitionsWithIndex((idx, iter) => {
      if (idx == partitionMap.length - 1) {
        iter.map(f => (f._1, f._2.map(_._2))) ++ (referenceNameBounds.keys.toSet -- distinctReferenceNamesCovered).toList.sorted
          .map(f => (referenceNameBounds(f), Iterable.empty[T]))
      } else {
        iter.map(f => (f._1, f._2.map(_._2)))
      }
    })
    // no longer need the intermediate result
    existingComplements.unpersist()
    allComplements.cache()
  }

  /**
    * Gets the complement from the pre-merged, presorted RDD.
    *
    * @param premergedRdd The pre-merged rdd.
    * @param partitionMap The partition map after merge.
    * @return An RDD of regions not represented in the RDD.
    */
  protected def getComplement(premergedRdd: RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])],
                              partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]): RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])] = {

    val sortedReferenceNames = referenceNameBounds.keys.toSeq.sorted
    premergedRdd.mapPartitionsWithIndex((idx, iter) => {
      if (iter.nonEmpty) {
        val next = iter.next

        val startPosition = if(idx == 0) {
          0
        } else {
          partitionMap.apply(idx - 1).get._2.start
        }
        val startRegion =
          (next._1, (ReferenceRegion(next._1.referenceName, startPosition, next._1.start), next._2))

        val localComplement = iter.foldLeft(List(startRegion))((b, a) => {
          val previousRegion = b.head._1
          val currentRegion = a._1

          if (previousRegion.referenceName == currentRegion.referenceName) {
            (currentRegion,
              (ReferenceRegion(previousRegion.referenceName,
                previousRegion.end, currentRegion.start),
              a._2)) :: b
            // there's a bit of complexity in the case that we are in between
            // referenceNames
          } else {
            val indexOfNext = sortedReferenceNames.indexOf(currentRegion.referenceName)
            val indexDifference = indexOfNext - sortedReferenceNames.indexOf(previousRegion.referenceName)
            // this is the case that there are referenceNames that are not
            // represented in the data and in the middle of the data
            val unrepresentedReferenceNames = if (indexDifference != 1) {
              for (i <- 1 until indexDifference) yield {
                (referenceNameBounds(sortedReferenceNames(indexOfNext + i)),
                  (referenceNameBounds(sortedReferenceNames(indexOfNext + i)),
                    a._2))
              }
            } else {
              List()
            }
            (currentRegion,
              (ReferenceRegion(currentRegion.referenceName, 0, currentRegion.start),
                a._2
                )) :: unrepresentedReferenceNames.toList ++
              ((previousRegion,
                  (ReferenceRegion(previousRegion.referenceName,
                    previousRegion.end,
                    referenceNameBounds(previousRegion.referenceName).end),
                    b.head._2._2
                    )) :: b)
          }
        })

        // this is necessary to bridge the gap between partitions
        val interNodeEnds =
          if (idx < partitionMap.length - 1 &&
            !partitionMap(idx + 1).exists(_._1.referenceName != localComplement.head._1.referenceName)) {
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