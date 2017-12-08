/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
