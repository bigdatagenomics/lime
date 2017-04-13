package org.bdgenomics.lime.util

import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.ReferenceRegion

/**
 * Created by DevinPetersohn on 4/13/17.
 */
private[lime] object Partitioners {
  class ReferenceRegionRangePartitioner[V](partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions

    def getPartition(key: Any): Int = {
      key match {
        case (_: Int, f2: Int)             => f2
        case (_: ReferenceRegion, f2: Int) => f2
        case _                             => throw new Exception("Unable to partition without destination assignment")
      }
    }
  }
}
