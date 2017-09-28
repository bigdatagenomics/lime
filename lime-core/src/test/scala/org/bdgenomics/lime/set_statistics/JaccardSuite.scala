package org.bdgenomics.lime.set_statistics

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.LimeFunSuite

class JaccardSuite extends LimeFunSuite {
  sparkTest("test jaccard distance between two regions") {
    val leftGenomicRDD = sc.loadFeatures(resourcesFile("/intersect_with_overlap_00.bed")).sortLexicographically()
    val rightGenomicRdd = sc.loadFeatures(resourcesFile("/intersect_with_overlap_01.bed")).sortLexicographically()

    val jaccard_dist = new JaccardDistance(leftGenomicRDD, rightGenomicRdd).compute()

    assert(JaccardStatistic(9240, 32917, 0.28070601816690466, 3).equals(jaccard_dist))
  }
}
