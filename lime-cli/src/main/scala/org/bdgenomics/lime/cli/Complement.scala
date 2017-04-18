package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.lime.set_theory
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument

object Complement extends BDGCommandCompanion {
  val commandName = "complement"
  val commandDescription = "Returns the intervals of a genome that is not covered by the input file"
  def apply(cmdLine: Array[String]) = {
    new Complement(Args4j[ComplementArgs](cmdLine))
  }

  class ComplementArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs{
    @Argument(required = true,
      metaVar = "INPUT1",
      usage = "The input file to compare against the genome",
      index = 0)
    var leftInput: String = null
    @Argument(required = true,
      metaVar = "INPUT2",
      usage = "The genome file",
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

    }
  }
}
