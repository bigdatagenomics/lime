package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.LimeFunSuite

class IntersectionSuite extends LimeFunSuite {
  sparkTest("test intersection between multiple overlapping regions") {
    val leftFile = sc.loadBed(resourcesFile("/intersect_with_overlap_00.bed")).repartitionAndSort()
    val rightFile = sc.loadBed(resourcesFile("/intersect_with_overlap_01.bed"))
    val intersection = DistributedIntersection(
      leftFile.rdd.map(f => (ReferenceRegion.unstranded(f), f)),
      rightFile.rdd.map(f => (ReferenceRegion.unstranded(f), f)),
      leftFile.partitionMap.get)
      .compute()

    val bedtoolsOuput = Array(ReferenceRegion("chr1", 135124, 135444),
      ReferenceRegion("chr1", 135124, 135563),
      ReferenceRegion("chr1", 135333, 135563),
      ReferenceRegion("chr1", 135453, 135563),
      ReferenceRegion("chr1", 135453, 135777))

    val zippedWithCorrectOutput = intersection.map(_._1).collect().zip(bedtoolsOuput)
    zippedWithCorrectOutput.foreach(println)
    assert(!zippedWithCorrectOutput.exists(f => f._1 != f._2))
  }
}
