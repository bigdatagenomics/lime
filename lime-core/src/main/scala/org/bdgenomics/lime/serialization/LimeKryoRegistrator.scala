package org.bdgenomics.lime.serialization

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator

class LimeKryoRegistrator extends KryoRegistrator {

  private val akr = new ADAMKryoRegistrator()

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.models.ReferenceRegion]])
    kryo.register(classOf[org.bdgenomics.adam.models.ReferenceRegion],
      new org.bdgenomics.adam.models.ReferenceRegionSerializer())

    // register adam's requirements
    //akr.registerClasses(kryo)

    kryo.register(classOf[org.bdgenomics.adam.models.SequenceRecord])
    kryo.register(classOf[scala.Array[org.bdgenomics.adam.models.SequenceRecord]])

    kryo.register(classOf[org.bdgenomics.formats.avro.Feature],
      new org.bdgenomics.adam.serialization.AvroSerializer[org.bdgenomics.formats.avro.Feature]())

    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[java.lang.Class[_]])

    kryo.register(classOf[scala.Array[scala.Option[_]]])
  }
}