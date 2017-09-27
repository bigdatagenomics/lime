package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.GenericGenomicRDD
import scala.reflect.ClassTag

private[set_theory] abstract class SetTheory[T, X, RT, RU] extends Serializable {

  protected def join()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(RT, RU)]

  protected def predicate(joinedTuple: (RT, RU)): (RT, RU)

  protected def regionPredicate(regions: ((RT, RU)) => Seq[ReferenceRegion]): ((RT, RU)) => Seq[ReferenceRegion]

  def compute()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): GenericGenomicRDD[(RT, RU)] = {
    val genomicRDD = join().transform(f => f.map(predicate))

    GenericGenomicRDD(genomicRDD.rdd,
      genomicRDD.sequences,
      regionPredicate(genomicRDD.regionFn),
      genomicRDD.optPartitionMap)
  }
}

private[set_theory] abstract class SingleCollectionSetTheory[T, RT] extends Serializable {

  protected def join()(implicit tTag: ClassTag[T]): GenericGenomicRDD[(T, RT)]

  protected def predicate(joinedTuple: (T, RT)): (T, RT)

  protected def reducePredicate(a: (T, RT), b: (T, RT)): (T, RT)

  protected def regionPredicate(regions: ((T, RT)) => Seq[ReferenceRegion]): ((T, RT)) => Seq[ReferenceRegion]

  def compute()(implicit tTag: ClassTag[T]): GenericGenomicRDD[(T, RT)] = {
    val genomicRDD = join().transform(f => f.map(predicate))

    genomicRDD.rdd.cache

    val x: RDD[(T, RT)] = {
      val z =
        genomicRDD.rdd
          .zipWithIndex
          .filter(f => regionPredicate(genomicRDD.regionFn)(f._1).nonEmpty)
          .values.mapPartitions(iter => {
            if (iter.isEmpty) {
              Iterator((-1L, -1L))
            } else {
              val n = iter.next

              if (iter.isEmpty) {
                Iterator((n, n))
              } else {
                Iterator((n, iter.max))
              }
            }
          }).collect

      genomicRDD.rdd.zipWithIndex().mapPartitionsWithIndex((idx, iter) => {
        var temp = -1L
        iter.map(f => {
          if (regionPredicate(genomicRDD.regionFn)(f._1).isEmpty) {
            if (temp == -1) {
              var i = idx
              while (i >= 0 && z(i)._2 == -1) {
                i -= 1
              }
              temp = z(i)._2
            }
            (temp, f._1)
          } else {
            temp = f._2
            f.swap
          }
        })
      }).reduceByKey((a: (T, RT), b: (T, RT)) => reducePredicate(a, b))
        .values
    }

    GenericGenomicRDD(
      x,
      genomicRDD.sequences,
      regionPredicate(genomicRDD.regionFn),
      genomicRDD.optPartitionMap)
  }
}