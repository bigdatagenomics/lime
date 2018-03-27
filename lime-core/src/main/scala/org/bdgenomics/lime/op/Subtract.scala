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
package org.bdgenomics.lime.op

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.{ GenericGenomicDataset, GenomicDataset }

import scala.reflect.ClassTag

sealed abstract class Subtract[T, U <: GenomicDataset[T, U], X, Y <: GenomicDataset[X, Y]] extends SetTheory[T, X, T, Iterable[X]] {
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

case class ShuffleSubtract[T, U <: GenomicDataset[T, U], X, Y <: GenomicDataset[X, Y]](
    leftRdd: GenomicDataset[T, U],
    rightRdd: GenomicDataset[X, Y],
    threshold: Long = 0L) extends Subtract[T, U, X, Y] {

  override protected def join()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicDataset[(T, Iterable[X])] = {
    leftRdd.leftOuterShuffleRegionJoinAndGroupByLeft[X, Y](rightRdd, threshold)
  }
}
