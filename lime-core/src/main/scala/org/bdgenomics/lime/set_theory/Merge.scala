package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.reflect.ClassTag

sealed abstract class Merge[T: ClassTag] extends SetTheoryWithSingleCollection[T] {
  def primitive(currRegion: ReferenceRegion,
                tempRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): ReferenceRegion = {

    currRegion.merge(tempRegion, distanceThreshold)
  }
}

case class DistributedMerge[T: ClassTag](rddToCompute: RDD[(ReferenceRegion, T)],
                                         partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                         distanceThreshold: Long = 0L) extends Merge[T] {

}