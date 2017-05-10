package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.lime.set_theory.SingleClosest
import org.bdgenomics.utils.cli.Args4j
import org.bdgenomics.utils.cli.Args4jBase
import org.bdgenomics.utils.cli.BDGCommandCompanion
import org.bdgenomics.utils.cli.BDGSparkCommand
import org.bdgenomics.utils.cli.ParquetArgs
import org.kohsuke.args4j.Argument

object Closest extends BDGCommandCompanion {
  val commandName = "closest"
  val commandDescription = "Compute closest regions between two inputs"

  def apply(cmdLine: Array[String]) = {
    new Closest(Args4j[ClosestArgs](cmdLine))
  }

  class ClosestArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
    @Argument(required = true,
      metaVar = "INPUT1",
      usage = "The first file for closest",
      index = 0)
    var leftInput: String = null
    @Argument(required = true,
      metaVar = "INPUT2",
      usage = "The second file for closest",
      index = 1)
    var rightInput: String = null

    override var sortFastqOutput: Boolean = false
    override var asSingleFile: Boolean = false
    override var deferMerging: Boolean = false
    override var outputPath: String = ""
    override var disableFastConcat: Boolean = false
  }

  class Closest(protected val args: ClosestArgs) extends BDGSparkCommand[ClosestArgs] {
    val companion = Closest

    def run(sc: SparkContext) {
      val leftGenomicRDD = sc.loadBed(args.leftInput)
        .repartitionAndSort()

      val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.stranded(f), f))
      val rightGenomicRDD = sc.loadBed(args.rightInput)
        .rdd
        .map(f => (ReferenceRegion.stranded(f), f))

      new SingleClosest(leftGenomicRDDKeyed, rightGenomicRDD, leftGenomicRDD.partitionMap.get)
        .compute()
        .collect()
        .foreach(println)
    }
  }
}
