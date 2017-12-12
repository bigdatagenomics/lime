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
package org.bdgenomics.lime.statistics

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.lime.LimeFunSuite

class JaccardSuite extends LimeFunSuite {
  sparkTest("test jaccard distance between two regions") {
    val leftGenomicRDD = sc.loadFeatures(resourcesFile("/intersect_with_overlap_00.bed")).sortLexicographically()
    val rightGenomicRdd = sc.loadFeatures(resourcesFile("/intersect_with_overlap_01.bed")).sortLexicographically()

    val jaccard_dist = new JaccardDistance(leftGenomicRDD, rightGenomicRdd).compute()

    assert(JaccardStatistic(9240, 32917, 0.28070601816690466, 3).equals(jaccard_dist))
  }
}
