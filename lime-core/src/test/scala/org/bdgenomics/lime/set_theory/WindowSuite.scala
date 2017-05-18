package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.LimeFunSuite

class WindowSuite extends LimeFunSuite {
  sparkTest("testing window matches bedtools output") {
    val leftFile = sc.loadBed(resourcesFile("/intersect_with_overlap_00.bed")).sortLexicographically()
    val rightFile = sc.loadBed(resourcesFile("/window_with_overlap_01.bed"))

    val windows = DistributedWindow(
      leftFile.rdd.map(f => (ReferenceRegion.unstranded(f), f)),
      rightFile.rdd.map(f => (ReferenceRegion.unstranded(f), f)),
      leftFile.optPartitionMap.get)
      .compute()
      .map(f =>
        (ReferenceRegion(f._2._1.getContigName, f._2._1.getStart, f._2._1.getEnd),
          ReferenceRegion(f._2._2.getContigName, f._2._2.getStart, f._2._2.getEnd)))
      .collect()

    val bedtoolsOutput = Array((ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135000, 135444)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135124, 135563)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135593, 135778)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135000, 135444)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135124, 135563)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135593, 135778)),
      (ReferenceRegion("chr1", 886356, 886602), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 894313, 902654), ReferenceRegion("chr1", 894313, 902654)))

    assert(!windows.zip(bedtoolsOutput).exists(f => f._1 != f._2))
  }
}
