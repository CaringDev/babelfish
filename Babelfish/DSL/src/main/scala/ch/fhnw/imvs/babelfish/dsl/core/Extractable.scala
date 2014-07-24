package ch.fhnw.imvs.babelfish.dsl.core

import ch.fhnw.imvs.babelfish.infrastructure.versioning.{ Versioned, VersionRangeSet }
import org.joda.time.DateTime
import scala.language.higherKinds
import scala.reflect.ClassTag

sealed trait Extractable[-T] {
  def name: String
  override def toString = s"Extractable[$name]"
}

abstract sealed class Versionable[-V: ClassTag](val name: String) extends Extractable[V] {
  def extract(a: Any, vrs: VersionRangeSet): Versioned[Any] = Versioned(vrs, a)
}

sealed case class SimpleExtractType[-E: ClassTag](override val name: String) extends Versionable[E](name)

final case class VersionedExtractType[-V](implicit val versioned: Versionable[V]) extends Extractable[Versioned[V]] {
  def extract(a: Any, vrs: VersionRangeSet): Versioned[Any] = a.asInstanceOf[Versioned[Any]].limit(vrs)
  val name = s"Versioned[${versioned.name}]"
}

final case class VectorExtractType[-E](implicit val inner: SimpleExtractType[E]) extends Versionable[Vector[E]](s"Vector[${inner.name}]")

final case class OptionExtractType[-E](implicit val inner: SimpleExtractType[E]) extends Versionable[Option[E]](s"Option[${inner.name}]")

object Extractable extends LowPrioExtractable {
  implicit object IntExtractor extends SimpleExtractType[Int]("Int")
  implicit object DoubleExtractor extends SimpleExtractType[Double]("Double")
  implicit object FloatExtractor extends SimpleExtractType[Float]("Float")
  implicit object LongExtractor extends SimpleExtractType[Long]("Long")
  implicit object BooleanExtractor extends SimpleExtractType[Boolean]("Boolean")
  implicit object StringExtractor extends SimpleExtractType[String]("String")
  implicit object DateTimeExtractor extends SimpleExtractType[DateTime]("DateTime")
  implicit object BigDecimalExtractor extends SimpleExtractType[BigDecimal]("BigDecimal")
  implicit object ByteExtractor extends SimpleExtractType[Byte]("Byte")
  implicit object CharExtractor extends SimpleExtractType[Char]("Char")
  implicit object ShortExtractor extends SimpleExtractType[Short]("Short")
  implicit def vectorExtractor[E: SimpleExtractType] = new VectorExtractType[E]
  implicit def optionExtractor[E: SimpleExtractType] = new OptionExtractType[E]
  implicit def versionedExtractor[V: Versionable]: Extractable[Versioned[V]] = new VersionedExtractType[V]
}

trait LowPrioExtractable {
  implicit def anyExtractor[E: ClassTag] = new SimpleExtractType[E]("Any") {
    override def extract(a: Any, vrs: VersionRangeSet) = Versioned(vrs, a)
  }
}