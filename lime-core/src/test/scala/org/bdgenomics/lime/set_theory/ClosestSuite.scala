package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.LimeFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._

class ClosestSuite extends LimeFunSuite {
  sparkTest("Testing closest with ties and multiple overlap matches bedtools output") {
    val leftFile = sc.loadBed(resourcesFile("/intersect_with_overlap_00.bed")).repartitionAndSort()
    val rightFile = sc.loadBed(resourcesFile("/intersect_with_overlap_01.bed"))

    val closestRdd = new SingleClosest(leftFile.flattenRddByRegions(), rightFile.flattenRddByRegions(), leftFile.partitionMap.get).compute()

    val closestResults = closestRdd.map(f => (ReferenceRegion(f._2._1.getContigName, f._2._1.getStart, f._2._1.getEnd),
      ReferenceRegion(f._2._2.getContigName, f._2._2.getStart, f._2._2.getEnd))).collect

    val bedToolsOutput = Array(
      (ReferenceRegion("chr1", 28735, 29810), ReferenceRegion("chr1", 135000, 135444)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135000, 135444)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135124, 135563)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135124, 135563)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 327790, 328229), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 437151, 438164), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 449273, 450544), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 533219, 534114), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 544738, 546649), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 713984, 714547), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 762416, 763445), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 788863, 789211), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 801975, 802338), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 805198, 805628), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 839694, 840619), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 844299, 845883), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 854765, 854973), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 858970, 861632), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 869332, 871872), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 875730, 878363), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 886356, 886602), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 894313, 902654), ReferenceRegion("chr1", 894313, 902654)))

    val zippedOutput = closestResults.zip(bedToolsOutput)
    assert(!zippedOutput.exists(f => f._1 != f._2))
  }

  sparkTest("Testing closest single highest overlap matches bedtools output") {
    val leftFile = sc.loadBed(resourcesFile("/intersect_with_overlap_00.bed")).repartitionAndSort()
    val rightFile = sc.loadBed(resourcesFile("/intersect_with_overlap_01.bed"))

    val closestRdd = SingleClosestSingleOverlap(leftFile.flattenRddByRegions(), rightFile.flattenRddByRegions(), leftFile.partitionMap.get).compute()

    val closestResults = closestRdd.map(f => (ReferenceRegion(f._2._1.getContigName, f._2._1.getStart, f._2._1.getEnd),
      ReferenceRegion(f._2._2.getContigName, f._2._2.getStart, f._2._2.getEnd))).collect

    val bedToolsOutput = Array(
      (ReferenceRegion("chr1", 28735, 29810), ReferenceRegion("chr1", 135000, 135444)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135124, 135563)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 327790, 328229), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 437151, 438164), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 449273, 450544), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 533219, 534114), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 544738, 546649), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 713984, 714547), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 762416, 763445), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 788863, 789211), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 801975, 802338), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 805198, 805628), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 839694, 840619), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 844299, 845883), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 854765, 854973), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 858970, 861632), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 869332, 871872), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 875730, 878363), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 886356, 886602), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 894313, 902654), ReferenceRegion("chr1", 894313, 902654)))

    val zippedOutput = closestResults.zip(bedToolsOutput)
    assert(!zippedOutput.exists(f => f._1 != f._2))
  }
}
