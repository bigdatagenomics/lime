package org.bdgenomics.lime.set_statistics

import scala.reflect.ClassTag

protected abstract class Statistic[T, X] {
  def compute()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): StatisticResult
}

protected abstract class StatisticResult {

  override def toString(): String
}