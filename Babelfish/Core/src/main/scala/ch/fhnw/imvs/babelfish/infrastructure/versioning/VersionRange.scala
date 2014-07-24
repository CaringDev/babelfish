package ch.fhnw.imvs.babelfish.infrastructure.versioning
import scala.language.implicitConversions

/** A version range [from,to] (both endpoints are inclusive).
  * Invariant: from <= to
  *
  * In order to reduce the number of property reads during traversal,
  * Both endpoints (from and to) are encoded into one long value.
  *
  * For reasons of efficiency VersionRange is encoded as a value class:
  * [[http://docs.scala-lang.org/sips/pending/value-classes.html]]
  *
  * @param fromTo a double containing both endpoints
  */
final class VersionRange private (val fromTo: Long) extends AnyVal {
  def from = new Version((fromTo >>> 32).toInt)
  def to = new Version((fromTo & 0x00000000FFFFFFFFl).toInt)
  def contains(v: Version): Boolean =
    from.v <= v.v && to.v >= v.v

  override def toString() = s"VersionRange($from -> $to)"
}

object VersionRange {
  /** Ordering instance used for sorting collections of VersionRanges. */
  implicit val versionRangeOrdering = new Ordering[VersionRange] {
    def compare(x: VersionRange, y: VersionRange) = x.fromTo.compare(y.fromTo)
  }

  def unapply(vr: VersionRange): Option[(Int, Int)] =
    Some((vr.from.v, vr.to.v))

  /** Creates a VersionRange [version, version] */
  def apply(version: Version): VersionRange =
    VersionRange(version, version)

  /** Creates a VersionRange [from, to] */
  def apply(from: Version, to: Version): VersionRange = {
    assert(from.v <= to.v, "Version.from must be less or equal Version.to.")
    new VersionRange((from.v.toLong << 32) | to.v)
  }

  private[infrastructure] def apply(fromTo: Long) =
    new VersionRange(fromTo)
}