package org.kiji.schema

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecordBuilder, GenericRecord}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.avro.specific.{SpecificRecordBuilderBase, SpecificRecord}
import java.lang.reflect.AccessibleObject
import java.nio.ByteBuffer
import com.google.gson.{JsonElement, JsonObject, JsonParser, Gson}
import org.apache.avro.data.RecordBuilderBase

trait Generator[T] {
  def next: T
}

// -------------------------------------------------------------------------------------------------

class NullGenerator() extends Generator[Null] {
  override val next = null
}

// -------------------------------------------------------------------------------------------------

class BooleanGenerator(mode: BooleanGenerator.Mode) extends Generator[Boolean] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => Boolean = mode match {
    case BooleanGenerator.AllTrue => () => true
    case BooleanGenerator.AllFalse => () => false
    case BooleanGenerator.ProbabilityTrue(probability) => () => rand.nextUniform(0, 1) < probability
  }
  def next: Boolean = generate()
}

object BooleanGenerator {
  sealed trait Mode
  case object AllTrue extends Mode
  case object AllFalse extends Mode
  case class ProbabilityTrue(probability: Double) extends Mode {
    require(probability >= 0 && probability <= 1)
  }
}

// -------------------------------------------------------------------------------------------------

class StringGenerator(mode: StringGenerator.Mode) extends Generator[String] {
  private val generate: () => String = mode match {
    case StringGenerator.Alphabetic(length) => () => RandomStringUtils.randomAlphabetic(length)
    case StringGenerator.Numeric(length) => () => RandomStringUtils.randomNumeric(length)
    case StringGenerator.Alphanumeric(length) => () => RandomStringUtils.randomAlphanumeric(length)
    case StringGenerator.Ascii(length) => () => RandomStringUtils.randomAscii(length)
    case StringGenerator.Enumerated(length, chars) => () => RandomStringUtils.random(length, chars)
  }
  override def next: String = generate()
}

object StringGenerator {
  sealed trait Mode
  case class Alphabetic(length: Int) extends Mode
  case class Numeric(length: Int) extends Mode
  case class Alphanumeric(length: Int) extends Mode
  case class Ascii(length: Int) extends Mode
  case class Enumerated(length: Int, chars: String) extends Mode
}

// -------------------------------------------------------------------------------------------------

class IntGenerator(mode: IntGenerator.Mode) extends Generator[Int] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => Int = mode match {
    case IntGenerator.Range(min, max) => () => rand.nextInt(min, max)
    case IntGenerator.Any => () => rand.nextInt(Int.MinValue, Int.MaxValue)
  }
  def next: Int = generate()
}

object IntGenerator {
  sealed trait Mode
  case class Range(min: Int, max: Int) extends Mode
  case object Any extends Mode
}

// -------------------------------------------------------------------------------------------------

class LongGenerator(mode: LongGenerator.Mode) extends Generator[Long] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => Long = mode match {
    case LongGenerator.Range(min, max) => () => rand.nextLong(min, max)
    case LongGenerator.Any => () => rand.nextLong(Long.MinValue, Long.MaxValue)
  }
  def next: Long = generate()
}

object LongGenerator {
  sealed trait Mode
  case class Range(min: Long, max: Long) extends Mode
  case object Any extends Mode
}

// -------------------------------------------------------------------------------------------------

class FloatGenerator(mode: FloatGenerator.Mode) extends Generator[Float] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => Float = mode match {
    case FloatGenerator.Range(min, max) => () => rand.nextUniform(min, max).toFloat
    case FloatGenerator.Any => () => rand.nextUniform(Float.MinValue, Float.MaxValue).toFloat
  }
  def next: Float = generate()
}

object FloatGenerator {
  sealed trait Mode
  case class Range(min: Float, max: Float) extends Mode
  case object Any extends Mode
}

// -------------------------------------------------------------------------------------------------

class DoubleGenerator(mode: DoubleGenerator.Mode) extends Generator[Double] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => Double = mode match {
    case DoubleGenerator.Range(min, max) => () => rand.nextUniform(min, max)
    case DoubleGenerator.Any => () => rand.nextUniform(Double.MinValue, Double.MaxValue)
  }
  def next: Double = generate()
}

object DoubleGenerator {
  sealed trait Mode
  case class Range(min: Double, max: Double) extends Mode
  case object Any extends Mode
}

// -------------------------------------------------------------------------------------------------

class BytesGenerator(mode: BytesGenerator.Mode) extends Generator[ByteBuffer] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => ByteBuffer = mode match {
    case BytesGenerator.Length(length) => () => ByteBuffer.wrap(rand.nextPermutation(256, length).map { _.toByte })
  }
  def next: ByteBuffer = generate()
}

object BytesGenerator {
  sealed trait Mode
  case class Length(length: Int) extends Mode
}

// -------------------------------------------------------------------------------------------------

class ArrayGenerator[T: ClassManifest](
    mode: ArrayGenerator.Mode,
    valueGenerator: Generator[T]
) extends Generator[Array[T]] {
  private val generate: () => Array[T] = mode match {
    case ArrayGenerator.Length(length) => () => new Array[T](length).map {
      (e: T) => valueGenerator.next
    }
  }
  def next: Array[T] = generate()
}

object ArrayGenerator {
  sealed trait Mode
  case class Length(length: Int) extends Mode
}

// -------------------------------------------------------------------------------------------------

class MapGenerator[T: ClassManifest](
    mode: MapGenerator.Mode,
    keyGenerator: Generator[String],
    valueGenerator: Generator[T]
) extends Generator[Map[String, T]] {
  private val generate: () => Map[String, T] = mode match {
    case MapGenerator.Length(length) => () => {
      val keys = new ArrayGenerator[String](ArrayGenerator.Length(length), keyGenerator).next
      val values = new ArrayGenerator[T](ArrayGenerator.Length(length), valueGenerator).next
      (keys zip values).toMap
    }
  }
  def next: Map[String, T] = generate()
}

object MapGenerator {
  sealed trait Mode
  case class Length(length: Int) extends Mode
}

// -------------------------------------------------------------------------------------------------

class EnumGenerator[T <: Enum[T]](mode: EnumGenerator.Mode) extends Generator[T] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => T = mode match {
    case EnumGenerator.Enumerated(values) => () => {
      values.apply(rand.nextInt(0, values.size - 1)).asInstanceOf[T]
    }
  }
  def next: T = generate()
}

object EnumGenerator {
  sealed trait Mode
  case class Enumerated[T <: Enum[T]](enum: Array[T]) extends Mode
}

// -------------------------------------------------------------------------------------------------

class FixedGenerator(mode: FixedGenerator.Mode) extends Generator[Array[Byte]] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => Array[Byte] = mode match {
    case FixedGenerator.Length(length) => () => rand.nextPermutation(256, length).map { _.toByte }
  }
  def next: Array[Byte] = generate()
}

object FixedGenerator {
  sealed trait Mode
  case class Length(length: Int) extends Mode
}

// -------------------------------------------------------------------------------------------------

class GenericRecordGenerator(mode: GenericRecordGenerator.Mode) extends Generator[GenericRecord] {
  private val generate: () => GenericRecord = mode match {
    case GenericRecordGenerator.Generic(schema, fields) => () => {
      val builder = new GenericRecordBuilder(schema)
      fields.foreach { pair: (String, Generator[_]) => builder.set(pair._1, pair._2.next)}
      builder.build()
    }
  }
  def next: GenericRecord = generate()
}

object GenericRecordGenerator {
  sealed trait Mode
  case class Generic(schema: Schema, fields: Map[String, Generator[_]]) extends Mode
}

class SpecificRecordGenerator[T <: SpecificRecord](
    mode: SpecificRecordGenerator.Mode
) extends Generator[T] {
  private val generate: () => T = mode match {
    case SpecificRecordGenerator.Specific(clazz, fields) => () => {
      val builder = clazz.getMethod("newBuilder").invoke(null)
      val fieldsMap = SpecificRecordGenerator.getFieldsFromSpecificRecordClass(clazz)
      fields.foreach { pair: (String, Generator[_]) => {
        val isFieldSetField = classOf[RecordBuilderBase[clazz.type]].getDeclaredField("fieldSetFlags")
        isFieldSetField.setAccessible(true)
        val field = builder.getClass.getDeclaredField(pair._1)
        field.setAccessible(true)
        field.set(builder, pair._2.next)
        val oldIsFieldSetField = isFieldSetField.get(builder).asInstanceOf[Array[Boolean]]
        oldIsFieldSetField(fieldsMap(pair._1)) = true
      }}
      builder.getClass.getFields.foreach(println)
      builder.asInstanceOf[SpecificRecordBuilderBase[T]].build
    }
  }
  def next: T = generate()
}

object SpecificRecordGenerator {
  sealed trait Mode
  case class Specific[T <: SpecificRecord](
      clazz: Class[T], fields: Map[String, Generator[_]]
  ) extends Mode

  def getFieldsFromSpecificRecordClass[T <: SpecificRecord](clazz: Class[T]): Map[String, Int] = {
    val schema = clazz.getMethod("getClassSchema").invoke(null).toString
    val obj = new JsonParser().parse(schema).asInstanceOf[JsonObject]
    import scala.collection.JavaConverters.asScalaSetConverter
    import scala.collection.JavaConverters.asScalaIteratorConverter
    import java.util.{Map => JMap}
    val jsonFields = obj.entrySet().asScala.map { (entry: JMap.Entry[String, JsonElement]) => (entry.getKey, entry.getValue)}.toMap.get("fields")
    val jsonFieldsArray = jsonFields.get.getAsJsonArray
    var i = -1
    jsonFieldsArray.iterator().asScala.map { (elem: JsonElement) => (elem.getAsJsonObject.get("name").getAsString, {i += 1; i})}.toMap
  }
}
