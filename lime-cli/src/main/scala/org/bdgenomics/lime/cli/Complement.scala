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
    override var disableFastConcat: Boolean = false
  }

  class Complement(protected val args: ComplementArgs) extends BDGSparkCommand[ComplementArgs] {
    val companion = Complement

    def run(sc: SparkContext) {
      println("Not supported")
      //      val leftGenomicRDD = sc.loadBed(args.leftInput)
      //      val genomeFile = sc.textFile(args.rightInput).map(_.split("\t"))
      //      val genomeMap = genomeFile.collect.map(f => f(0) -> ReferenceRegion(f(0), 0, f(1).toLong)).toMap
      //      val leftGenomicRDDKeyed = leftGenomicRDD.rdd.map(f => (ReferenceRegion.stranded(f), f))
      //
      //      ShuffleComplement(leftGenomicRDDKeyed, leftGenomicRDD.partitionMap.get, genomeMap)
      //        .compute()
      //        .rdd
      //        .map(_._1)
      //        .collect()
      //        .foreach(println)

    }
  }
}
