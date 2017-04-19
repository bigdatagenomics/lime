package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.LimeFunSuite

class SubtractSuite extends LimeFunSuite {
  sparkTest("test subtract between multiple overlapping regions") {
    val leftFile = sc.loadBed(resourcesFile("/intersect_with_overlap_00.bed")).repartitionAndSort()
    val rightFile = sc.loadBed(resourcesFile("/intersect_with_overlap_01.bed"))

    val subtraction = DistributedSubtract(leftFile.flattenRddByRegions(), rightFile.flattenRddByRegions(), leftFile.partitionMap.get)
      .compute()

    val regionsSubtracted = subtraction.map(_._1).collect

    val bedtoolsOutput = Array(ReferenceRegion("chr1", 28735, 29810),
      ReferenceRegion("chr1", 135777, 139441),
      ReferenceRegion("chr1", 327790, 328229),
      ReferenceRegion("chr1", 437151, 438164),
      ReferenceRegion("chr1", 449273, 450544),
      ReferenceRegion("chr1", 533219, 534114),
      ReferenceRegion("chr1", 544738, 546649),
      ReferenceRegion("chr1", 713984, 714547),
      ReferenceRegion("chr1", 762416, 763445),
      ReferenceRegion("chr1", 788863, 789211),
      ReferenceRegion("chr1", 801975, 802338),
      ReferenceRegion("chr1", 805198, 805628),
      ReferenceRegion("chr1", 839694, 840619),
      ReferenceRegion("chr1", 844299, 845883),
      ReferenceRegion("chr1", 854765, 854973),
      ReferenceRegion("chr1", 858970, 861632),
      ReferenceRegion("chr1", 869332, 871872),
      ReferenceRegion("chr1", 875730, 878363))

    assert(!regionsSubtracted.zip(bedtoolsOutput).exists(f => f._1 != f._2))
  }
}
