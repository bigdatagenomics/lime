package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.lime.set_theory.DistributedComplement
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument

object Complement extends BDGCommandCompanion {
  val commandName = "complement"
  val commandDescription = "Extract intervals not represented by an interval file."

  def apply(cmdLine: Array[String]) = {
    new Complement(Args4j[ComplementArgs](cmdLine))
  }

  class ComplementArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
    @Argument(required = true,
      metaVar = "INPUT1",
      usage = "The first file for intersection",
      index = 0)
    var leftInput: String = null
    @Argument(required = true,
      metaVar = "INPUT2",
      usage = "The second file for intersection",
      index = 1)
    var rightInput: String = null

    override var sortFastqOutput: Boolean = false
    override var asSingleFile: Boolean = false
    override var deferMerging: Boolean = false
    override var outputPath: String = ""
  }

  class Complement(protected val args: ComplementArgs) extends BDGSparkCommand[ComplementArgs] {
    val companion = Complement

    def run(sc: SparkContext) {
      val leftRdd = sc.loadBed(args.leftInput).repartitionAndSort()
      val genomeFile = sc.textFile(args.rightInput).map(_.split("\t"))
      val genomeMap = genomeFile.collect.map(f => f(0) -> ReferenceRegion(f(0), 0, f(1).toLong)).toMap

      val referenceRegionKeyedLeft = leftRdd.flattenRddByRegions()

      DistributedComplement(referenceRegionKeyedLeft, leftRdd.partitionMap.get, genomeMap)
        .compute()
        .map(_._1)
        .collect()
        .foreach(println)

    }
  }
}