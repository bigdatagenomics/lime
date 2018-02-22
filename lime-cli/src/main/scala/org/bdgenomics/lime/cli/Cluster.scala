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
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.TextRddWriter
import org.bdgenomics.lime.op.ShuffleCluster
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option ⇒ Args4jOption }

object Cluster extends BDGCommandCompanion {
  val commandName = "cluster"
  val commandDescription = "Cluster (but don’t merge) overlapping/nearby intervals"

  def apply(cmdLine: Array[String]) = {
    new Cluster(Args4j[ClusterArgs](cmdLine))
  }

  class ClusterArgs extends Args4jBase with ParquetSaveArgs {
    @Argument(required = true,
      metaVar = "INPUT",
      usage = "The features file to cluster (e.g., .bed, .gff/.gtf, .gff3, .interval_list, .narrowPeak). If extension is not detected, Parquet is assumed.",
      index = 0)
    var input: String = null

    @Argument(required = false,
      metaVar = "OUTPUT",
      usage = "The output file for cluster, if any. If not specified, cluster will collect on the driver node and write to stdout.",
      index = 1)
    var outputPath: String = null

    @Args4jOption(required = false,
      name = "-num_partitions",
      usage = "Number of partitions to use when loading the features file.")
    var numPartitions: Int = _

    @Args4jOption(required = false,
      name = "-single",
      usage = "Save as a single text file.")
    var asSingleFile: Boolean = false

    @Args4jOption(required = false,
      name = "-disable_fast_concat",
      usage = "Disables the parallel file concatenation engine.")
    var disableFastConcat: Boolean = false
  }

  class Cluster(protected val args: ClusterArgs) extends BDGSparkCommand[ClusterArgs] {
    val companion = Intersection

    def run(sc: SparkContext) {
      val leftGenomicRDD = sc.loadFeatures(
        args.input,
        optMinPartitions = Option(args.numPartitions),
        optProjection = None)

      val clusterRDD = ShuffleCluster(leftGenomicRDD)
        .compute()
        .rdd

      Option(args.outputPath).fold(
        clusterRDD
          .collect
          .foreach(println))(
          TextRddWriter.writeTextRdd(
            clusterRDD,
            _,
            asSingleFile = args.asSingleFile,
            disableFastConcat = args.disableFastConcat))
    }
  }
}
