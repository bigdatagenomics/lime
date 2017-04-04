package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument

object Sort extends BDGCommandCompanion {
  val commandName = "sort"
  val commandDescription = "Sort a file containing genomic data"

  def apply(cmdLine: Array[String]) = {
    new Sort(Args4j[SortArgs](cmdLine))
  }

  class SortArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
    @Argument(required = true,
      metaVar = "INPUT",
      usage = "The file to sort",
      index = 0)
    var input: String = null

    override var sortFastqOutput: Boolean = false
    override var asSingleFile: Boolean = false
    override var deferMerging: Boolean = false
    override var outputPath: String = ""
  }

  class Sort(protected val args: SortArgs) extends BDGSparkCommand[SortArgs] {
    val companion = Sort

    def run(sc: SparkContext) {
      val genomicRdd = sc.loadBed(args.input)

      genomicRdd.sort.rdd.collect.foreach(println)
    }
  }
}