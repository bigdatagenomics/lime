package org.bdgenomics.lime.set_statistics

import org.bdgenomics.adam.rdd.GenomicRDD
import org.bdgenomics.lime.set_theory.{ ShuffleIntersection, ShuffleMerge }

import scala.reflect.ClassTag

class JaccardDistance[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](leftRdd: GenomicRDD[T, U],
                                                                          rightRdd: GenomicRDD[X, Y]) extends Statistic[T, X] {

  def compute()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): StatisticResult = {

    val leftMergedRDD = ShuffleMerge(leftRdd).compute()
    val rightMergedRDD = ShuffleMerge(rightRdd).compute()

    val intersectedRdd = ShuffleIntersection(leftMergedRDD, rightMergedRDD)
      .compute()

    val intersectLength = intersectedRdd.rdd.flatMap(f => intersectedRdd.regionFn(f).map(_.length)).sum()
    val unionLength = leftMergedRDD.rdd.flatMap(f => leftMergedRDD.regionFn(f).map(_.length)).sum() + rightMergedRDD.rdd.flatMap(f => rightMergedRDD.regionFn(f).map(_.length)).sum()
    val jaccardDist = intersectLength / (unionLength - intersectLength)
    val nIntersections = intersectedRdd.rdd.count()

    JaccardStatistic(intersectLength.toLong, (unionLength - intersectLength).toLong, jaccardDist, nIntersections)

  }

}

private case class JaccardStatistic(intersectLength: Long,
                                    unionLength: Long,
                                    jaccardDist: Double,
                                    nIntersections: Long) extends StatisticResult {

  override def toString(): String = {

    "intersection\tunion-intersection\tjaccard\tn_intersections\n" +
      s"$intersectLength\t$unionLength\t$jaccardDist\t$nIntersections"
  }
}