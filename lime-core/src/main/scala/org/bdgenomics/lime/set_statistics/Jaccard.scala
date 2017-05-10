package org.bdgenomics.lime.set_statistics

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{ ADAMSaveAnyArgs, GenomicRDD, feature }
import org.bdgenomics.lime.set_theory.DistributedIntersection
import org.bdgenomics.lime.set_theory.DistributedMerge
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.feature.FeatureRDD

import scala.reflect.ClassTag

class JaccardDistance[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, Feature)],
                                                rightRdd: RDD[(ReferenceRegion, Feature)],
                                                leftPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                                rightPartitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]]) {

  def compute(): List[Any] = {

    val intersectedRdd = DistributedIntersection(leftRdd, rightRdd, leftPartitionMap)
      .compute().map(f => {

        val new_feature = new Feature(f._2._1.getFeatureId(), f._2._1.getName(), f._2._1.getSource(), f._2._1.getFeatureType(),
          f._2._1.getContigName(), f._1.start, f._1.end, f._2._1.getStrand(), f._2._1.getPhase(), f._2._1.getFrame(),
          f._2._1.getScore(), f._2._1.getGeneId(), f._2._1.getTranscriptId(), f._2._1.getExonId(), f._2._1.getAliases(),
          f._2._1.getParentIds(), f._2._1.getTarget(), f._2._1.getGap(), f._2._1.getDerivesFrom(), f._2._1.getNotes(), f._2._1.getDbxrefs(),
          f._2._1.getOntologyTerms(), f._2._1.getCircular(), f._2._1.getAttributes())

        (f._1, new_feature)

      })

    val mergedIntersectRDD = DistributedMerge(intersectedRdd, leftPartitionMap).compute()
    val intersect_length = mergedIntersectRDD.map(f => f._1.length()).sum()
    val leftmergedRDD = DistributedMerge(leftRdd, leftPartitionMap).compute()

    val rightmergedRDD = DistributedMerge(rightRdd, rightPartitionMap).compute()
    val union_length = leftmergedRDD.map(f => f._1.length()).sum() + rightmergedRDD.map(f => f._1.length()).sum()

    val jaccard_dist = intersect_length / (union_length - intersect_length)

    List(intersect_length, (union_length - intersect_length), jaccard_dist)

  }

}
