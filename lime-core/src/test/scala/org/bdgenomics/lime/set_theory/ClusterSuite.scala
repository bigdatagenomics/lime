package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.LimeFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._

class ClusterSuite extends LimeFunSuite {
  sparkTest("test local merge when all data merges to a single region") {
    val genomicRdd = sc.loadBed(resourcesFile("/cpg_20merge.bed")).sortLexicographically()
    val x = UnstrandedCluster(
      genomicRdd.rdd.map(f => (ReferenceRegion.unstranded(f), f)), genomicRdd.optPartitionMap.get)
      .compute()

    assert(x.count == 1)
  }
}
