package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.utils.cli.{ Args4j, Args4jBase, BDGCommandCompanion, BDGSparkCommand, ParquetArgs }
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
      println("Not supported")
      //      val leftGenomicRDD = sc.loadBed(args.leftInput)
      //
      //
      //      val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.stranded(f), f))
      //      val rightGenomicRDD = sc.loadBed(args.rightInput)
      //        .rdd
      //        .map(f => (ReferenceRegion.stranded(f), f))
      //
      //
      //      new SingleClosest(leftGenomicRDDKeyed, rightGenomicRDD)
      //        .compute()
      //        .collect()
      //        .foreach(println)

    }
  }
}
