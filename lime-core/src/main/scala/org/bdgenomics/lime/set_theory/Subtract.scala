package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.{ GenericGenomicRDD, GenomicRDD }

import scala.reflect.ClassTag

sealed abstract class Subtract[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]] extends SetTheory[T, X, T, Iterable[X]] {
  override protected def predicate(joinedTuple: (T, Iterable[X])): (T, Iterable[X]) = {
    joinedTuple
  }

  override protected def regionPredicate(regions: ((T, Iterable[X])) => Seq[ReferenceRegion]): ((T, Iterable[X])) => Seq[ReferenceRegion] = {

    regions.andThen(f => {
      if (f.length == 1) {
        f
      } else {
        f.tail.foldLeft(Seq(f.head))((a, b) => {
          if (a.isEmpty) {
            a
          } else {
            a.dropRight(1) ++ a.last.subtract(b)
          }
        })
      }
    })
  }
}

case class ShuffleSubtract[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](leftRdd: GenomicRDD[T, U],
                                                                               rightRdd: GenomicRDD[X, Y],
                                                                               threshold: Long = 0L) extends Subtract[T, U, X, Y] {

  override protected def join()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(T, Iterable[X])] = {
    leftRdd.leftOuterShuffleRegionJoinAndGroupByLeft[X, Y](rightRdd, threshold)
  }
}