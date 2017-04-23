package org.bdgenomics.lime.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.set_theory.DistributedWindow
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument

/**
  * Created by DevinPetersohn on 4/22/17.
  */
object Window extends BDGCommandCompanion {
  val commandName = "window"
  val commandDescription = "Compute nearby regions between two inputs"

  def apply(cmdLine: Array[String]) = {
    new Window(Args4j[WindowArgs](cmdLine))
  }

  class WindowArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
    @Argument(required = true,
      metaVar = "INPUT1",
      usage = "The first file for window",
      index = 0)
    var leftInput: String = null
    @Argument(required = true,
      metaVar = "INPUT2",
      usage = "The second file for window",
      index = 1)
    var rightInput: String = null

    override var sortFastqOutput: Boolean = false
    override var asSingleFile: Boolean = false
    override var deferMerging: Boolean = false
    override var outputPath: String = ""
  }

  class Window(protected val args: WindowArgs) extends BDGSparkCommand[WindowArgs] {
    val companion = Window

    def run(sc: SparkContext) {
      val leftGenomicRDD = sc.loadBed(args.leftInput).repartitionAndSort()
      val rightGenomicRDD = sc.loadBed(args.rightInput).copartitionByReferenceRegion(leftGenomicRDD)

      DistributedWindow(leftGenomicRDD.flattenRddByRegions, rightGenomicRDD.flattenRddByRegions, leftGenomicRDD.partitionMap.get)
        .compute()
        .collect()
        .foreach(println)
    }
  }
}
