package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.lime.set_theory.UnstrandedCluster
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument

object Cluster extends BDGCommandCompanion {
  val commandName = "cluster"
  val commandDescription = "Cluster (but don’t merge) overlapping/nearby intervals"

  def apply(cmdLine: Array[String]) = {
    new Cluster(Args4j[ClusterArgs](cmdLine))
  }

  class ClusterArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
    @Argument(required = true,
      metaVar = "INPUT",
      usage = "The input file for cluster",
      index = 0)
    var input: String = null

    override var sortFastqOutput: Boolean = false
    override var asSingleFile: Boolean = false
    override var deferMerging: Boolean = false
    override var outputPath: String = ""
    override var disableFastConcat: Boolean = false
  }

  class Cluster(protected val args: ClusterArgs) extends BDGSparkCommand[ClusterArgs] {
    val companion = Intersection

    def run(sc: SparkContext) {
      val leftGenomicRDD = sc.loadBed(args.input).repartitionAndSort()
      val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.stranded(f), f))

      UnstrandedCluster(leftGenomicRDDKeyed, leftGenomicRDD.partitionMap.get)
        .compute()
        .collect
        .foreach(println)
    }
  }
}
