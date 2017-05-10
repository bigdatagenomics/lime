package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{ ADAMSaveAnyArgs, GenomicRDD, feature }
import org.bdgenomics.lime.set_theory.DistributedIntersection
import org.bdgenomics.lime.set_theory.DistributedMerge
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.lime.cli
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
  }

  class Jaccard(protected val args: JaccardArgs) extends BDGSparkCommand[JaccardArgs] {
    val companion = Jaccard

    def run(sc: SparkContext) {
      val leftGenomicRDD = sc.loadFeatures(args.leftInput).repartitionAndSort()

      val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.unstranded(f), f))

      val rightGenomicRdd = sc.loadFeatures(args.rightInput).repartitionAndSort()

      val rightGenomicRDDKeyed = rightGenomicRdd.rdd.map(f => (ReferenceRegion.unstranded(f), f))

      val jaccard_dist = new JaccardDistance(leftGenomicRDDKeyed, rightGenomicRDDKeyed, leftGenomicRDD.partitionMap.get,
        rightGenomicRdd.partitionMap.get).compute()
      println("$ Intersection $ Union-Intersection $ Jaccard")
      println("$ " + jaccard_dist(0) + " $ " + jaccard_dist(1) + " $ " + jaccard_dist(2))

    }
  }

}
