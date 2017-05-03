package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument

object Coverage extends BDGCommandCompanion {
  val commandName = "coverage"
  val commandDescription = "Compute the coverage over defined intervals"

  def apply(cmdLine: Array[String]) = {
    new Coverage(Args4j[CoverageArgs](cmdLine))
  }

  class CoverageArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
    @Argument(required = true,
      metaVar = "INPUT1",
      usage = "The first file for intersection",
      index = 0)
    var input: String = null

    override var sortFastqOutput: Boolean = false
    override var asSingleFile: Boolean = false
    override var deferMerging: Boolean = false
    override var outputPath: String = ""
    //override var disableFastConcat: Boolean = false
  }

  class Coverage(protected val args: CoverageArgs) extends BDGSparkCommand[CoverageArgs] {
    val companion = Intersection

    def run(sc: SparkContext) {
      val leftGenomicRDD = sc.loadBed(args.input).toCoverage
      leftGenomicRDD.rdd.collect.foreach(println)
    }
  }
}
