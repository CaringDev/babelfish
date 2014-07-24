package ch.fhnw.imvs.babelfish.schema.neo4j

import scala.language.existentials
import scala.language.higherKinds

abstract class CombinedNeoType[V[_], T](implicit val inner: SimpleNeoType[T]) extends NeoType[V[T]] {

  def prefix: String

  override def equals(obj: Any) = obj match {
    case other: CombinedNeoType[V, T] => (this eq other) ||
      other.inner == inner && prefix == other.prefix
    case _ => false
  }

  def stringRepresentation = s"$prefix(${inner.stringRepresentation})"

  override def hashCode() = (17 + inner.hashCode) * 31 + prefix.hashCode
}

trait CombinedNeoTypes {

  //Parse is in JsonFormat with [a,...,o]
  final class VectorTypeDecl[T: SimpleNeoType: scala.reflect.ClassTag] extends CombinedNeoType[Vector, T] {

    val prefix = "Vector"
    type dbType = Array[inner.dbType]

    def parseFromString(value: String): Option[Vector[T]] = {
      if (value != null && value.startsWith("[") && value.endsWith("]")) {
        if (value != "[]") {
          val striped = value.drop(1).dropRight(1)

          val split = striped.split(",").toVector
          val v = split.map(inner.parseFromString)
          if (v.forall(_.isDefined)) {
            Some(v.map(_.get))
          } else {
            None
          }
        } else {
          Some(Vector[T]())
        }
      } else {
        None
      }
    }

    def toDbRep(t: Any): dbType =
      t.asInstanceOf[Vector[T]].map(inner.toDbRep).toArray(inner.dbTypeTag)

    def fromDbRep(any: Any): Vector[T] =
      any.asInstanceOf[dbType].toVector.map(inner.fromDbRep)

    override def equalsDbRep(a: Any, b: Any): Boolean =
      a.asInstanceOf[dbType].sameElements(b.asInstanceOf[dbType])
  }

  implicit def vectorIsNeoType[T: SimpleNeoType: scala.reflect.ClassTag]: NeoType[Vector[T]] = new VectorTypeDecl[T]

  final class OptionTypeDecl[T: SimpleNeoType] extends CombinedNeoType[Option, T] {

    val prefix = "Option"
    type dbType = Array[inner.dbType]

    def parseFromString(value: String): Option[Option[T]] = {
      value match {
        case null   => None
        case "None" => Some(None)
        case v if v.startsWith("[") && v.endsWith("]") => {
          val stripped = v.substring(1, v.length - 1)
          inner.parseFromString(stripped).map(e => Some(e))
        }
        case _ => None
      }
    }

    def toDbRep(t: Any) = t match {
      case None    => Array[inner.dbType]()(inner.dbTypeTag)
      case Some(a) => Array[inner.dbType](inner.toDbRep(a))(inner.dbTypeTag)
    }

    def fromDbRep(any: Any) = any match {
      case Array()  => None
      case Array(a) => Some(inner.fromDbRep(a))
    }

    override def equalsDbRep(a: Any, b: Any): Boolean =
      a.asInstanceOf[dbType].sameElements(b.asInstanceOf[dbType])
  }

  implicit def optionIsNeoType[T: SimpleNeoType]: NeoType[Option[T]] = new OptionTypeDecl[T]
}