package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.{ GenericGenomicRDD, GenomicRDD }

import scala.reflect.ClassTag

sealed private[set_theory] abstract class Window[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]] extends SetTheory[T, X, T, X] {

  override protected def predicate(joinedTuple: (T, X)): (T, X) = {
    joinedTuple
  }

  override protected def regionPredicate(regions: ((T, X)) => Seq[ReferenceRegion]): ((T, X)) => Seq[ReferenceRegion] = regions
}

case class ShuffleWindow[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](leftRdd: GenomicRDD[T, U],
                                                                             rightRdd: GenomicRDD[X, Y],
                                                                             threshold: Long = 1000L) extends Window[T, U, X, Y] {

  override protected def join()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(T, X)] = {
    leftRdd.shuffleRegionJoin[X, Y](rightRdd, threshold)
  }
}

//case class BroadcastWindow[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](leftRdd: GenomicRDD[T, U],
//                                                                               rightRdd: GenomicRDD[X, Y],
//                                                                               threshold: Long = 1000L) extends Window[T, U, X, Y] {
//  override protected def join()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(T, Iterable[X])] = {
//    rightRdd.broadcastRegionJoinAndGroupByRight[T, U](leftRdd).transmute(f => f.map(_.swap))
//  }
//}