package org.kiji.schema

import org.kiji.schema.avro.{KeyValueBackupEntry, LocalityGroupDesc, CompressionType}

object App {

  def main(args : Array[String]) {
    val gen = new SpecificRecordGenerator[KeyValueBackupEntry](SpecificRecordGenerator.Specific(classOf[KeyValueBackupEntry], Map(
      "timestamp" -> new LongGenerator(LongGenerator.Range(0, Long.MaxValue)),
      "key" -> new StringGenerator(StringGenerator.Alphabetic(5)),
      "value" -> new BytesGenerator(BytesGenerator.Length(10)))))
    val KVBE = gen.next
    println(KVBE)

//    val gen = new GenericRecordGenerator(GenericRecordGenerator.Generic(KeyValueBackupEntry.getClassSchema,
//        Map(
//          "timestamp" -> new LongGenerator(LongGenerator.Range(0, Long.MaxValue)),
//          "key" -> new StringGenerator(StringGenerator.Alphabetic(5)),
//          "value" -> new StringGenerator(StringGenerator.Numeric(5)))))
//    println(gen.next)

//    val gen = new EnumGenerator(EnumGenerator.Enumerated(CompressionType.values()))
//    for (i <- Range(0, 10)) {
//      println(gen.next)
//    }

//    val gen = new MapGenerator[Int](MapGenerator.Length(10), new StringGenerator(StringGenerator.Alphabetic(5)), new IntGenerator(IntGenerator.Any))
//    for (i <- Range(0, 100)) {
//      gen.next.foreach { println }
//      println(gen.next)
//    }

//    val gen = new BytesGenerator(BytesGenerator.Length(10))
//    for (i <- Range(0, 100000)) {
//      gen.next.foreach { b: Byte => assert(b < 128 && b >= -128, "%d".format(b)) }
//    }
  }

}
