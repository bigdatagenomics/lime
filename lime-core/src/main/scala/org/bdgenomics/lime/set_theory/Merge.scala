package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.reflect.ClassTag

sealed abstract class Merge[T: ClassTag] extends SetTheoryWithSingleCollection[T] {
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
}

case class DistributedMerge[T: ClassTag](rddToCompute: RDD[(ReferenceRegion, T)],
                                         partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                         threshold: Long = 0L) extends Merge[T]