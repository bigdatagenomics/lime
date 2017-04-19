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

  /**
   * Post processing in Merge does nothing, as the key already represents
   * the value.
   *
   * @param rdd The RDD after computation is complete.
   * @return The RDD after post-processing.
   */
  protected def postProcess(rdd: RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])]): RDD[(ReferenceRegion, Iterable[T])] = {
    rdd.map(f => (f._1, f._2.map(_._2)))
  }
}

case class DistributedMerge[T: ClassTag](@transient rddToCompute: RDD[(ReferenceRegion, T)],
                                         @transient partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                         @transient threshold: Long = 0L) extends Merge[T]