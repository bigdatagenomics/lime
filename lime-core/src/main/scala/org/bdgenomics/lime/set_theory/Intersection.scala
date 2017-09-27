package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.{ GenericGenomicRDD, GenomicRDD }

import scala.reflect.ClassTag

sealed abstract class Intersection[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]] extends SetTheory[T, X, T, X] {

  override protected def predicate(joinedTuple: (T, X)): (T, X) = {
    joinedTuple
  }

  override protected def regionPredicate(regions: ((T, X)) => Seq[ReferenceRegion]): ((T, X)) => Seq[ReferenceRegion] = {

    regions.andThen(f => {
      if (f.length == 1) {
        Seq()
      } else {
        Seq(f(0).intersection(f(1)))
      }
    })
  }
}

case class ShuffleIntersection[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](leftRdd: GenomicRDD[T, U],
                                                                                   rightRdd: GenomicRDD[X, Y],
                                                                                   threshold: Long = 0L) extends Intersection[T, U, X, Y] {
  override protected def join()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(T, X)] = {
    leftRdd.shuffleRegionJoin[X, Y](rightRdd, threshold)
  }
}
