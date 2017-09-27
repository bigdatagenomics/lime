/**
 * Created by DevinPetersohn on 4/11/17.
 */

package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.LimeFunSuite

class MergeSuite extends LimeFunSuite {
  sparkTest("test local merge when all data merges to a single region") {
    val genomicRdd = sc.loadBed(resourcesFile("/cpg_20merge.bed"))
    val x = ShuffleMerge(
      genomicRdd)
      .compute()
    assert(x.rdd.count == 1)
  }
}