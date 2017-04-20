package org.bdgenomics.lime.set_theory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.LimeFunSuite

class ComplementSuite extends LimeFunSuite {
  sparkTest("testing complement on large bed file") {
    //val leftRdd = sc.loadBed("/Users/DevinPetersohn/Downloads/bedtools_data/exons.bed").repartitionAndSort()
    val leftRdd = sc.loadBed(resourcesFile("/cpg_20merge.bed")).repartitionAndSort()
    val genomeFile = sc.textFile("/Users/DevinPetersohn/Downloads/bedtools_data/genome.txt").map(_.split("\t"))
    val genomeMap = genomeFile.collect.map(f => f(0) -> ReferenceRegion(f(0), 0, f(1).toLong)).toMap
    val referenceRegionKeyedLeft = leftRdd.flattenRddByRegions()

    val complements = DistributedComplement(referenceRegionKeyedLeft, leftRdd.partitionMap.get, genomeMap)
      .compute()
      .map(_._1)
      .collect()

    val bedtoolsOutput = Array(ReferenceRegion("chr1", 0, 28735),
      ReferenceRegion("chr1", 30000, 249250621),
      ReferenceRegion("chr10", 0, 135534747),
      ReferenceRegion("chr11", 0, 135006516),
      ReferenceRegion("chr11_gl000202_random", 0, 40103),
      ReferenceRegion("chr12", 0, 133851895),
      ReferenceRegion("chr13", 0, 115169878),
      ReferenceRegion("chr14", 0, 107349540),
      ReferenceRegion("chr15", 0, 102531392),
      ReferenceRegion("chr16", 0, 90354753),
      ReferenceRegion("chr17", 0, 81195210),
      ReferenceRegion("chr17_ctg5_hap1", 0, 1680828),
      ReferenceRegion("chr17_gl000203_random", 0, 37498),
      ReferenceRegion("chr17_gl000204_random", 0, 81310),
      ReferenceRegion("chr17_gl000205_random", 0, 174588),
      ReferenceRegion("chr17_gl000206_random", 0, 41001),
      ReferenceRegion("chr18", 0, 78077248),
      ReferenceRegion("chr18_gl000207_random", 0, 4262),
      ReferenceRegion("chr19", 0, 59128983),
      ReferenceRegion("chr19_gl000208_random", 0, 92689),
      ReferenceRegion("chr19_gl000209_random", 0, 159169),
      ReferenceRegion("chr1_gl000191_random", 0, 106433),
      ReferenceRegion("chr1_gl000192_random", 0, 547496),
      ReferenceRegion("chr2", 0, 243199373),
      ReferenceRegion("chr20", 0, 63025520),
      ReferenceRegion("chr21", 0, 48129895),
      ReferenceRegion("chr21_gl000210_random", 0, 27682),
      ReferenceRegion("chr22", 0, 51304566),
      ReferenceRegion("chr3", 0, 198022430),
      ReferenceRegion("chr4", 0, 191154276),
      ReferenceRegion("chr4_ctg9_hap1", 0, 590426),
      ReferenceRegion("chr4_gl000193_random", 0, 189789),
      ReferenceRegion("chr4_gl000194_random", 0, 191469),
      ReferenceRegion("chr5", 0, 180915260),
      ReferenceRegion("chr6", 0, 171115067),
      ReferenceRegion("chr6_apd_hap1", 0, 4622290),
      ReferenceRegion("chr6_cox_hap2", 0, 4795371),
      ReferenceRegion("chr6_dbb_hap3", 0, 4610396),
      ReferenceRegion("chr6_mann_hap4", 0, 4683263),
      ReferenceRegion("chr6_mcf_hap5", 0, 4833398),
      ReferenceRegion("chr6_qbl_hap6", 0, 4611984),
      ReferenceRegion("chr6_ssto_hap7", 0, 4928567),
      ReferenceRegion("chr7", 0, 159138663),
      ReferenceRegion("chr7_gl000195_random", 0, 182896),
      ReferenceRegion("chr8", 0, 146364022),
      ReferenceRegion("chr8_gl000196_random", 0, 38914),
      ReferenceRegion("chr8_gl000197_random", 0, 37175),
      ReferenceRegion("chr9", 0, 141213431),
      ReferenceRegion("chr9_gl000198_random", 0, 90085),
      ReferenceRegion("chr9_gl000199_random", 0, 169874),
      ReferenceRegion("chr9_gl000200_random", 0, 187035),
      ReferenceRegion("chr9_gl000201_random", 0, 36148),
      ReferenceRegion("chrM", 0, 16571),
      ReferenceRegion("chrUn_gl000211", 0, 166566),
      ReferenceRegion("chrUn_gl000212", 0, 186858),
      ReferenceRegion("chrUn_gl000213", 0, 164239),
      ReferenceRegion("chrUn_gl000214", 0, 137718),
      ReferenceRegion("chrUn_gl000215", 0, 172545),
      ReferenceRegion("chrUn_gl000216", 0, 172294),
      ReferenceRegion("chrUn_gl000217", 0, 172149),
      ReferenceRegion("chrUn_gl000218", 0, 161147),
      ReferenceRegion("chrUn_gl000219", 0, 179198),
      ReferenceRegion("chrUn_gl000220", 0, 161802),
      ReferenceRegion("chrUn_gl000221", 0, 155397),
      ReferenceRegion("chrUn_gl000222", 0, 186861),
      ReferenceRegion("chrUn_gl000223", 0, 180455),
      ReferenceRegion("chrUn_gl000224", 0, 179693),
      ReferenceRegion("chrUn_gl000225", 0, 211173),
      ReferenceRegion("chrUn_gl000226", 0, 15008),
      ReferenceRegion("chrUn_gl000227", 0, 128374),
      ReferenceRegion("chrUn_gl000228", 0, 129120),
      ReferenceRegion("chrUn_gl000229", 0, 19913),
      ReferenceRegion("chrUn_gl000230", 0, 43691),
      ReferenceRegion("chrUn_gl000231", 0, 27386),
      ReferenceRegion("chrUn_gl000232", 0, 40652),
      ReferenceRegion("chrUn_gl000233", 0, 45941),
      ReferenceRegion("chrUn_gl000234", 0, 40531),
      ReferenceRegion("chrUn_gl000235", 0, 34474),
      ReferenceRegion("chrUn_gl000236", 0, 41934),
      ReferenceRegion("chrUn_gl000237", 0, 45867),
      ReferenceRegion("chrUn_gl000238", 0, 39939),
      ReferenceRegion("chrUn_gl000239", 0, 33824),
      ReferenceRegion("chrUn_gl000240", 0, 41933),
      ReferenceRegion("chrUn_gl000241", 0, 42152),
      ReferenceRegion("chrUn_gl000242", 0, 43523),
      ReferenceRegion("chrUn_gl000243", 0, 43341),
      ReferenceRegion("chrUn_gl000244", 0, 39929),
      ReferenceRegion("chrUn_gl000245", 0, 36651),
      ReferenceRegion("chrUn_gl000246", 0, 38154),
      ReferenceRegion("chrUn_gl000247", 0, 36422),
      ReferenceRegion("chrUn_gl000248", 0, 39786),
      ReferenceRegion("chrUn_gl000249", 0, 38502),
      ReferenceRegion("chrX", 0, 155270560),
      ReferenceRegion("chrY", 0, 59373566))

    assert(!complements.zip(bedtoolsOutput).exists(f => f._1 != f._2))
  }
}
