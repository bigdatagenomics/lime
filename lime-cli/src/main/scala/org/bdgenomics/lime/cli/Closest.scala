/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
