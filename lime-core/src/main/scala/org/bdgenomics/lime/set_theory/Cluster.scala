package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.{ GenomicRDD, GenericGenomicRDD }

import scala.reflect.ClassTag

sealed abstract class Cluster[T, U <: GenomicRDD[T, U]] extends SingleCollectionSetTheory[T, Iterable[T]] {

  override protected def predicate(joinedTuple: (T, Iterable[T])): (T, Iterable[T]) = joinedTuple

  override protected def reducePredicate(a: (T, Iterable[T]), b: (T, Iterable[T])): (T, Iterable[T]) = {
    (a._1, (a._2 ++ b._2).toStream.distinct.toIterable)
  }

  override protected def regionPredicate(regions: ((T, Iterable[T])) => Seq[ReferenceRegion]): ((T, Iterable[T])) => Seq[ReferenceRegion] = {
    regions.andThen(f => {
      if (f(0) != f(1)) {
        Seq()
      } else {
        f
      }
    })
  }
}

case class ShuffleCluster[T, U <: GenomicRDD[T, U]](genomicRdd: GenomicRDD[T, U],
                                                    threshold: Long = 0L) extends Cluster[T, U] {

  override protected def join()(implicit tTag: ClassTag[T]): GenericGenomicRDD[(T, Iterable[T])] = {
    genomicRdd.shuffleRegionJoinAndGroupByLeft(genomicRdd, threshold)
  }
}
