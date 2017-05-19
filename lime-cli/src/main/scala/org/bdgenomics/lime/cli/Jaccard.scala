package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{ ADAMSaveAnyArgs, GenomicRDD, feature }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.lime.set_statistics.JaccardDistance

object Jaccard extends BDGCommandCompanion {
  val commandName = "jaccard"
  val commandDescription = "Compute jaccard distance between two inputs"

  def apply(cmdLine: Array[String]) = {
    new Jaccard(Args4j[JaccardArgs](cmdLine))
  }

  class JaccardArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
    @Argument(required = true,
      metaVar = "INPUT1",
      usage = "The first file for Jaccard",
      index = 0)
    var leftInput: String = null
    @Argument(required = true,
      metaVar = "INPUT2",
      usage = "The second file for Jaccard",
      index = 1)
    var rightInput: String = null

    override var sortFastqOutput: Boolean = false
    override var asSingleFile: Boolean = false
    override var deferMerging: Boolean = false
    override var outputPath: String = ""
    override var disableFastConcat: Boolean = false
  }

  class Jaccard(protected val args: JaccardArgs) extends BDGSparkCommand[JaccardArgs] {
    val companion = Jaccard

    def run(sc: SparkContext) {

      val leftGenomicRDD = sc.loadFeatures(args.leftInput).sortLexicographically()
      val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.unstranded(f), f))
      val rightGenomicRdd = sc.loadFeatures(args.rightInput).sortLexicographically()
      val rightGenomicRDDKeyed = rightGenomicRdd.rdd.map(f => (ReferenceRegion.unstranded(f), f))

      val jaccard_dist = new JaccardDistance(leftGenomicRDDKeyed, rightGenomicRDDKeyed, leftGenomicRDD.optPartitionMap.get,
        rightGenomicRdd.optPartitionMap.get).compute()
      println(jaccard_dist)

    }
  }

}
