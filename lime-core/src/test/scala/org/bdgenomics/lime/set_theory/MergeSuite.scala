/**
 * Created by DevinPetersohn on 4/11/17.
 */

package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.LimeFunSuite

class MergeSuite extends LimeFunSuite {
  sparkTest("test local merge when all data merges to a single region") {
    val genomicRdd = sc.loadBed(resourcesFile("/cpg_20merge.bed")).sortLexicographically()
    val x = DistributedMerge(
      genomicRdd.rdd.map(f => (ReferenceRegion.unstranded(f), f)),
      genomicRdd.optPartitionMap.get)
      .compute()
    assert(x.count == 1)
  }
}