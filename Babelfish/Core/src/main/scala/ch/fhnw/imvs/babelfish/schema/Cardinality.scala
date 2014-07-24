package ch.fhnw.imvs.babelfish.schema

/** Models the cardinality of a schema edge. */
sealed trait Cardinality

object Cardinality {
  /** 1..n */
  case object + extends Cardinality
  /** 0..1*/
  case object ? extends Cardinality
  /** 0..n */
  case object * extends Cardinality
  /** 1..1 */
  case object One extends Cardinality {
    override def toString = "1"
  }
}