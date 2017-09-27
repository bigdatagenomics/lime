package org.bdgenomics.lime.set_theory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import scala.reflect.ClassTag

sealed abstract class Complement[T: ClassTag] {

}

case class DistributedComplement[T: ClassTag](rddToCompute: RDD[(ReferenceRegion, T)],
                                              partitionMap: Array[Option[(ReferenceRegion, ReferenceRegion)]],
                                              referenceNameBounds: Map[String, ReferenceRegion],
                                              threshold: Long = 0L) extends Complement[T]