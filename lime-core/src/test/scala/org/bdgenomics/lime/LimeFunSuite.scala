package org.bdgenomics.lime

/**
 * Created by DevinPetersohn on 4/11/17.
 */
import org.bdgenomics.utils.misc.SparkFunSuite

class LimeFunSuite extends SparkFunSuite {
  override val appName: String = "Lime"
  override val properties: Map[String, String] = Map(("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
    ("spark.kryo.registrator", "org.bdgenomics.lime.serialization.LimeKryoRegistrator"),
    ("spark.kryoserializer.buffer", "4M"),
    ("spark.kryo.referenceTracking", "true"))

  def resourcesFile(path: String) = getClass.getResource(path).getFile
}
