package org.bdgenomics.lime.set_statistics

import org.apache.spark.rdd.RDD
import org.bdgenomics.lime.set_theory.{ DistributedIntersection, DistributedMerge }
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.adam.models.ReferenceRegion

import scala.reflect.ClassTag

class JaccardDistance[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, Feature)],
                                                rightRdd: RDD[(ReferenceRegion, Feature)],
                                                leftPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                rightPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]) extends Statistic {

  def compute(): StatisticResult = {

    val leftmergedRDD = DistributedMerge(leftRdd, leftPartitionMap).compute()
    val rightmergedRDD = DistributedMerge(rightRdd, rightPartitionMap).compute()

    val intersectedRdd = DistributedIntersection(leftmergedRDD, rightmergedRDD, leftPartitionMap)
      .compute().map(f => f._1)

    val intersectLength = intersectedRdd.map(f => f.length()).sum()
    val unionLength = leftmergedRDD.map(f => f._1.length()).sum() + rightmergedRDD.map(f => f._1.length()).sum()
    val jaccardDist = intersectLength / (unionLength - intersectLength)
    val nIntersections = intersectedRdd.count()

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