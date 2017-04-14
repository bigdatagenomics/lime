package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.reflect.ClassTag

sealed abstract class Intersect[T: ClassTag] extends SetTheory[T] {
  def primitive(currRegion: ReferenceRegion,
                tempRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): ReferenceRegion = {

    currRegion.intersection(tempRegion)
  }
}

case class DistributedIntersect[T: ClassTag](rddToCompute: RDD[(ReferenceRegion, T)],
                                             partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                             distanceThreshold: Long = 0L) extends Intersect[T]