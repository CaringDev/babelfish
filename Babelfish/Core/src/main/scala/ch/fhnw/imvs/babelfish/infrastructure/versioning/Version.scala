package ch.fhnw.imvs.babelfish.infrastructure.versioning
import scala.language.implicitConversions

/** A single version number.
  *
  * For reasons of efficiency Version is encoded as a value class:
  * [[http://docs.scala-lang.org/sips/pending/value-classes.html]]
  */
final class Version(val v: Int) extends AnyVal {
  /** Returns the previous version. */
  def previous = new Version(v - 1)
  /** Returns the next version. */
  def next = new Version(v + 1)
  override def toString = if (v == Version.Head.v) "Head" else v.toString
}

object Version {
  val Head = Version(Int.MaxValue)
  implicit def apply(v: Int): Version = new Version(v)
}