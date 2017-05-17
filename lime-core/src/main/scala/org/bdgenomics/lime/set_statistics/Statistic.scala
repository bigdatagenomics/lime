package org.bdgenomics.lime.set_statistics

protected abstract class Statistic {
  def compute(): StatisticResult
}

protected abstract class StatisticResult {
  override def toString(): String
}