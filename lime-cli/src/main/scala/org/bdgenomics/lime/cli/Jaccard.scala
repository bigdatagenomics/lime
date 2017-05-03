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
      val leftGenomicRDD = sc.loadBed(args.leftInput).repartitionAndSort()
      val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.unstranded(f), f))
      val rightGenomicRdd = sc.loadBed(args.rightInput).repartitionAndSort()
      val rightGenomicRDDKeyed = rightGenomicRdd.rdd
        .map(f => (ReferenceRegion.unstranded(f), f))

      //Intersection of the two files and merging the result to calculate intersect_lengthe
      val intersectedRdd = DistributedIntersection(leftGenomicRDDKeyed, rightGenomicRDDKeyed, leftGenomicRDD.partitionMap.get)
        .compute().map(f => {

          val new_feature = new Feature(f._2._1.getFeatureId(), f._2._1.getName(), f._2._1.getSource(), f._2._1.getFeatureType(),
            f._2._1.getContigName(), f._1.start, f._1.end, f._2._1.getStrand(), f._2._1.getPhase(), f._2._1.getFrame(),
            f._2._1.getScore(), f._2._1.getGeneId(), f._2._1.getTranscriptId(), f._2._1.getExonId(), f._2._1.getAliases(),
            f._2._1.getParentIds(), f._2._1.getTarget(), f._2._1.getGap(), f._2._1.getDerivesFrom(), f._2._1.getNotes(), f._2._1.getDbxrefs(),
            f._2._1.getOntologyTerms(), f._2._1.getCircular(), f._2._1.getAttributes())

          (f._1, new_feature)
        })
      val premergedIntersectRDD = FeatureRDD.inferSequenceDictionary(intersectedRdd.map(_._2), Some(StorageLevel.MEMORY_ONLY))
      val mergedIntersectRDD = DistributedMerge(premergedIntersectRDD.rdd.map(f => (ReferenceRegion.unstranded(f), f)), leftGenomicRDD.partitionMap.get).compute()
      val intersect_length = mergedIntersectRDD.map(f => f._1.length()).sum()

      val tempRightRDD = rightGenomicRdd.repartitionAndSort()

      //Merging the two input files for union cacluclation
      val leftmergedRDD = DistributedMerge(leftGenomicRDDKeyed, leftGenomicRDD.partitionMap.get).compute()

      val rightmergedRDD = DistributedMerge(rightGenomicRDDKeyed, rightGenomicRdd.partitionMap.get).compute()
      //Union Calculation

      val union_length = leftmergedRDD.map(f => f._1.length()).sum() + rightmergedRDD.map(f => f._1.length()).sum()

      val jaccard = intersect_length / (union_length - intersect_length)

      println(intersect_length + " " + (union_length - intersect_length) + " " + jaccard)

    }
  }

}
