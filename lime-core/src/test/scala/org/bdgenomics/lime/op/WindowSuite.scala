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
package org.bdgenomics.lime.op

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.lime.LimeFunSuite

class WindowSuite extends LimeFunSuite {
  sparkTest("testing window matches bedtools output") {
    val leftFile: FeatureRDD = sc.loadBed(resourcesFile("/intersect_with_overlap_00.bed"))
    val rightFile: FeatureRDD = sc.loadBed(resourcesFile("/window_with_overlap_01.bed"))

    val windows = ShuffleWindow(
      leftFile,
      rightFile)
      .compute()
      .rdd.map(f =>
        (ReferenceRegion(f._1.getContigName, f._1.getStart, f._1.getEnd),
          ReferenceRegion(f._2.getContigName, f._2.getStart, f._2.getEnd)))
      .collect()

    val bedtoolsOutput = Array((ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135000, 135444)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135124, 135563)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135593, 135778)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135000, 135444)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135124, 135563)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135333, 135777)),
      (ReferenceRegion("chr1", 135453, 139441), ReferenceRegion("chr1", 135593, 135778)),
      (ReferenceRegion("chr1", 886356, 886602), ReferenceRegion("chr1", 886356, 886602)),
      (ReferenceRegion("chr1", 894313, 902654), ReferenceRegion("chr1", 894313, 902654)))
    assert(!windows.sorted.zip(bedtoolsOutput.sorted).exists(f => f._1 != f._2))
  }
}
