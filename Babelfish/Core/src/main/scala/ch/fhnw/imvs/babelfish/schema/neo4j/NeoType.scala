package ch.fhnw.imvs.babelfish.schema.neo4j

/** NeoType is a Typeclass which allows a type to be used in the Neo4J database
  *
  * it transforms types between two representation, the JVM representation and the Neo4J Representation.
  *
  * @tparam T is the type which should be enabled for use in the Neo4J DB
  */
trait NeoType[T] {
  type dbType
  def parseFromString(value: String): Option[T]

  /** @return a machine interpretable string representation of this [[ch.fhnw.imvs.babelfish.schema.neo4j.NeoType]] */
  def stringRepresentation: String

  override def toString =
    s"NeoType($stringRepresentation)"

  /** @return a human readable string representation of this [[ch.fhnw.imvs.babelfish.schema.neo4j.NeoType]] */
  def name = stringRepresentation

  def toDbRep(t: Any): dbType
  def fromDbRep(any: Any): T

  /** Compares two DbRep values of the same type. */
  def equalsDbRep(a: Any, b: Any): Boolean =
    fromDbRep(a) == fromDbRep(b)
}

/** NeoType companion object */
object NeoType extends SimpleNeoTypes with CombinedNeoTypes