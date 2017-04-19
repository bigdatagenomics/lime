package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion

import scala.reflect.ClassTag

sealed abstract class Cluster[T: ClassTag] extends SetTheoryWithSingleCollection[T] {

  /**
   * In Cluster, there is no primitive operation, but we have to merge to
   * continue extending our cluster. This is taken care of in post-processing.
   *
   * @param firstRegion The first region for the primitive.
   * @param secondRegion The second region for the primitive.
   * @param distanceThreshold The threshold for the primitive.
   * @return The computed primitive for the two regions.
   */
  def primitive(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): ReferenceRegion = {

    firstRegion.merge(secondRegion, distanceThreshold)
  }

  /**
   * Post processes clustered regions such that the first region in the
   * cluster is the key.
   *
   * @param rdd The RDD after computation is complete.
   * @return The RDD after post-processing.
   */
  protected def postProcess(rdd: RDD[(ReferenceRegion, Iterable[(ReferenceRegion, T)])]): RDD[(ReferenceRegion, Iterable[T])] = {
    rdd.map(f => (f._2.head._1, f._2.map(_._2)))
  }
}

case class UnstrandedCluster[T: ClassTag](@transient rddToCompute: RDD[(ReferenceRegion, T)],
                                          @transient partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                          @transient threshold: Long = 0L) extends Cluster[T] {

  /**
   * UnstrandedCluster does not enforce strandedness, so we use cover.
   *
   * @param firstRegion The region to test against.
   * @param secondRegion The region to test.
   * @param distanceThreshold The threshold for the primitive.
   * @return True if the threshold requirement is met.
   *         False if the threshold requirement is not met.
   */
  def condition(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): Boolean = {

    firstRegion.covers(secondRegion, distanceThreshold)
  }
}

case class StrandedCluster[T: ClassTag](@transient rddToCompute: RDD[(ReferenceRegion, T)],
                                        @transient partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                        @transient threshold: Long = 0L) extends Cluster[T] {

  /**
   * StrandedCluster requires the same strand, so overlap is used here.
   *
   * @param firstRegion The region to test against.
   * @param secondRegion The region to test.
   * @param distanceThreshold The threshold for the primitive.
   * @return True if the threshold requirement is met.
   *         False if the threshold requirement is not met.
   */
  def condition(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): Boolean = {

    firstRegion.overlaps(secondRegion, distanceThreshold)
  }
}

case class UnstrandedClusterWithMinimumOverlap[T: ClassTag](@transient rddToCompute: RDD[(ReferenceRegion, T)],
                                                            @transient partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                            @transient threshold: Long = 0L) extends Cluster[T] {

  /**
   * UnstrandedClusterWithMinimumOverlap does not require identical stranded,
   * but does require a minimum overlap.
   *
   * @param firstRegion The region to test against.
   * @param secondRegion The region to test.
   * @param distanceThreshold The threshold for the primitive.
   * @return True if the threshold requirement is met.
   *         False if the threshold requirement is not met.
   */
  def condition(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): Boolean = {

    firstRegion.coversBy(secondRegion).exists(_ >= distanceThreshold)
  }
}

case class StrandedClusterWithMinimumOverlap[T: ClassTag](@transient rddToCompute: RDD[(ReferenceRegion, T)],
                                                          @transient partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                          @transient threshold: Long = 0L) extends Cluster[T] {
  /**
   * StrandedClusterWithMinimumOverlap requires both strand identity and
   * minimum overlap.
   *
   * @param firstRegion The region to test against.
   * @param secondRegion The region to test.
   * @param distanceThreshold The threshold for the primitive.
   * @return True if the threshold requirement is met.
   *         False if the threshold requirement is not met.
   */
  def condition(firstRegion: ReferenceRegion,
                secondRegion: ReferenceRegion,
                distanceThreshold: Long = 0L): Boolean = {

    firstRegion.overlapsBy(secondRegion).exists(_ >= distanceThreshold)
  }
}
