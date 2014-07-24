package ch.fhnw.imvs.babelfish.schema.neo4j

import ch.fhnw.imvs.babelfish.util.Logging
import org.joda.time.DateTime
import scala.reflect.{ ClassTag, classTag }
import scala.util.Try

trait SimpleNeoType[T] extends NeoType[T] {
  def typeTag: ClassTag[T]
  def dbTypeTag: ClassTag[dbType]
  def toDbRep(t: Any): dbType = t.asInstanceOf[dbType]
  def fromDbRep(a: Any): T = a.asInstanceOf[T]
}

trait SimpleNeoTypes extends Logging {

  implicit case object BooleanTypeDecl extends SimpleNeoType[Boolean] {
    def parseFromString(value: String): Option[Boolean] = if (value != null) value.toLowerCase match {
      case "true" | "1"  => Some(true)
      case "false" | "0" => Some(false)
      case _             => None
    }
    else None

    type dbType = Boolean
    val dbTypeTag = classTag[Boolean]
    val typeTag = classTag[Boolean]

    val stringRepresentation = "Boolean"
  }

  implicit case object ByteTypeDecl extends SimpleNeoType[Byte] {
    def parseFromString(value: String): Option[Byte] = try {
      Some(java.lang.Byte.parseByte(value))
    } catch {
      case nfe: NumberFormatException => None
      case np: NullPointerException   => None
    }

    type dbType = Byte
    val dbTypeTag = classTag[Byte]
    val typeTag = classTag[Byte]

    val stringRepresentation = "Byte"
  }

  implicit case object ShortTypeDecl extends SimpleNeoType[Short] {

    def parseFromString(value: String): Option[Short] = try {
      Some(java.lang.Short.parseShort(value))
    } catch {
      case nfe: NumberFormatException => None
      case np: NullPointerException   => None
    }

    type dbType = Short
    val dbTypeTag = classTag[Short]
    val typeTag = classTag[Short]

    val stringRepresentation = "Short"
  }

  implicit case object IntTypeDecl extends SimpleNeoType[Int] {
    def parseFromString(value: String): Option[Int] = try {
      Some(java.lang.Integer.parseInt(value))
    } catch {
      case nfe: NumberFormatException => None
      case np: NullPointerException   => None
    }

    type dbType = Int
    val dbTypeTag = classTag[Int]
    val typeTag = classTag[Int]

    val stringRepresentation = "Int"
  }

  implicit case object LongTypeDecl extends SimpleNeoType[Long] {
    def parseFromString(value: String): Option[Long] = Try { java.lang.Long.parseLong(value) }.toOption

    type dbType = Long
    val dbTypeTag = classTag[Long]
    val typeTag = classTag[Long]

    val stringRepresentation = "Long"
  }

  implicit case object FloatTypeDecl extends SimpleNeoType[Float] {
    def parseFromString(value: String): Option[Float] = try {
      Some(java.lang.Float.parseFloat(value))
    } catch {
      case nfe: NumberFormatException => None
      case np: NullPointerException   => None
    }

    type dbType = Float
    val dbTypeTag = classTag[Float]
    val typeTag = classTag[Float]

    val stringRepresentation = "Float"
  }

  implicit case object DoubleTypeDecl extends SimpleNeoType[Double] {
    def parseFromString(value: String): Option[Double] = try {
      Some(java.lang.Double.parseDouble(value))
    } catch {
      case nfe: NumberFormatException => None
      case np: NullPointerException   => None
    }

    type dbType = Double
    val dbTypeTag = classTag[Double]
    val typeTag = classTag[Double]

    val stringRepresentation = "Double"
  }

  implicit case object BigDecimalTypeDecl extends SimpleNeoType[BigDecimal] {
    def parseFromString(value: String): Option[BigDecimal] = try {
      Some(BigDecimal(value))
    } catch {
      case nfe: NumberFormatException => None
      case np: NullPointerException   => None
    }

    type dbType = String
    val dbTypeTag = classTag[String]
    val typeTag = classTag[BigDecimal]

    val stringRepresentation = "BigDecimal"

    override def toDbRep(t: Any): dbType = t.toString()
    override def fromDbRep(any: Any): BigDecimal = BigDecimal(any.asInstanceOf[String])
  }

  implicit case object CharTypeDecl extends SimpleNeoType[Char] {
    def parseFromString(value: String): Option[Char] = if (value != null && value.length == 1) Some(value.charAt(0)) else None

    type dbType = Char
    val dbTypeTag = classTag[Char]
    val typeTag = classTag[Char]

    val stringRepresentation = "Char"
  }

  implicit case object StringTypeDecl extends SimpleNeoType[String] {
    def parseFromString(value: String): Option[String] = Option(value) //option to make None from null

    type dbType = String
    val dbTypeTag = classTag[String]
    val typeTag = classTag[String]

    val stringRepresentation = "String"
  }

  implicit case object DateTimeTypeDecl extends SimpleNeoType[DateTime] {
    def parseFromString(value: String): Option[DateTime] = Try { org.joda.time.DateTime.parse(value) }.toOption

    type dbType = Long
    val dbTypeTag = classTag[Long]
    val typeTag = classTag[DateTime]

    val stringRepresentation = "DateTime"

    override def toDbRep(t: Any): dbType = t.asInstanceOf[DateTime].getMillis
    override def fromDbRep(any: Any): DateTime = new DateTime(any.asInstanceOf[Long])
  }

  case class EnumNeoType[T <: Enumeration](enum: T)(implicit tvTag: ClassTag[T#Value]) extends SimpleNeoType[T#Value] {

    type dbType = String

    override def name = "Enum:" + enum.toString

    def parseFromString(value: String) =
      Try(enum.withName(value)).orElse(Try(enum(value.toInt))).toOption

    override def toDbRep(t: Any) = t.toString
    override def fromDbRep(any: Any) = (enum: T).withName(any.asInstanceOf[String])

    val typeTag = tvTag
    val dbTypeTag = classTag[String]
    val stringRepresentation = "Enum:" + enum.getClass.getName
  }
}