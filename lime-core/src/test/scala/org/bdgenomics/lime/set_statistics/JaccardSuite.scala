package org.bdgenomics.lime.set_statistics

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.LimeFunSuite

class JaccardSuite extends LimeFunSuite {
  sparkTest("test jaccard distance between two regions") {
    val leftGenomicRDD = sc.loadFeatures(resourcesFile("/intersect_with_overlap_00.bed")).repartitionAndSort()
    val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.unstranded(f), f))
    val rightGenomicRdd = sc.loadFeatures(resourcesFile("/intersect_with_overlap_01.bed")).repartitionAndSort()
    val rightGenomicRDDKeyed = rightGenomicRdd.rdd.map(f => (ReferenceRegion.unstranded(f), f))

    val jaccard_dist = new JaccardDistance(leftGenomicRDDKeyed, rightGenomicRDDKeyed, leftGenomicRDD.partitionMap.get,
      rightGenomicRdd.partitionMap.get).compute()
    assert(List(9240, 32917, 0.28070601816690466).equals(jaccard_dist))
  }
}
