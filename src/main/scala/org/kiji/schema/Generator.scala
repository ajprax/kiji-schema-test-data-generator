package org.kiji.schema

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.math3.random.RandomDataGenerator


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

class BytesGenerator(mode: BytesGenerator.Mode) extends Generator[Array[Byte]] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => Array[Byte] = mode match {
    case BytesGenerator.Length(length) => () => rand.nextPermutation(256, length).map { _.toByte }
  }
  def next: Array[Byte] = generate()
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

class EnumGenerator(mode: EnumGenerator.Mode) extends Generator[String] {
  private val rand: RandomDataGenerator = new RandomDataGenerator()
  private val generate: () => String = mode match {
    case EnumGenerator.Enumerated(values) => () => {
      val ordered: List[String] = values.toList
      ordered(rand.nextInt(0, values.size - 1))
    }
  }
  def next: String = generate()
}

object EnumGenerator {
  sealed trait Mode
  case class Enumerated(values: Set[String]) extends Mode
}
