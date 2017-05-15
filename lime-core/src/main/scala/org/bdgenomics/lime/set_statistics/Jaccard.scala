package org.bdgenomics.lime.set_statistics

import org.apache.spark.rdd.RDD
import org.bdgenomics.lime.set_theory.DistributedIntersection
import org.bdgenomics.lime.set_theory.DistributedMerge
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.adam.models.ReferenceRegion


import scala.reflect.ClassTag

class JaccardDistance[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, Feature)],
                                                rightRdd: RDD[(ReferenceRegion, Feature)],
                                                leftPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                rightPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]) {

  def compute(): List[Any] = {

    val leftmergedRDD = DistributedMerge(leftRdd, leftPartitionMap).compute()

    val rightmergedRDD = DistributedMerge(rightRdd, rightPartitionMap).compute()

    val intersectedRdd = DistributedIntersection(leftmergedRDD, rightmergedRDD, leftPartitionMap)
      .compute().map(f => {

        (f._1, f._2._1)

      })

    val intersect_length = intersectedRdd.map(f => f._1.length()).sum()

    val union_length = leftmergedRDD.map(f => f._1.length()).sum() + rightmergedRDD.map(f => f._1.length()).sum()

    val jaccard_dist = intersect_length / (union_length - intersect_length)

    List(intersect_length, (union_length - intersect_length), jaccard_dist)

  }

}
