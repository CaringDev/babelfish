package ch.fhnw.imvs.babelfish.infrastructure.versioning

import scala.language.implicitConversions
import scala.annotation.tailrec

/** A set of version ranges (intervals).
  * Invariant:
  * - contained versions do not overlap (1..4, 3..6 is illegal)
  * - contained versions are not contiguous (1..4, 5..6 is illegal)
  * - versions is sorted in ascending order
  *
  * @param versions the sorted intervals
  */
final class VersionRangeSet private (val versions: Vector[VersionRange]) {
  def isEmpty: Boolean = versions.isEmpty
  def nonEmpty: Boolean = versions.nonEmpty

  override def hashCode: Int = versions.hashCode()
  override def equals(a: Any): Boolean = a match {
    case v: VersionRangeSet => versions == v.versions
    case _                  => false
  }

  override def toString: String = s"VersionRangeSet(${versions.map(v => (v.from, v.to)).mkString(",")})"

  /** Computes the intersection of this and the given argument.
    *
    * @param vrs the VersionRangeSet to be intersected with this
    * @return the intersection of this and vrs
    */
  def intersect(vrs: VersionRangeSet): VersionRangeSet = {
    // This algorithm depends on the fact that the VersionRanges within a VersionRangeSet are sorted in ascending order.

    @tailrec
    def rec(vrss1: Vector[VersionRange], vrss2: Vector[VersionRange], acc: Vector[VersionRange]): Vector[VersionRange] = (vrss1, vrss2) match {
      case (Vector(), _) => acc
      case (_, Vector()) => acc
      case (vrs1, vrs2) =>
        // VersionRange.unapply incurs costs of ~ 20% of query execution time, need to investigate
        // val VersionRange(f1, t1) = vrs1(0)
        // val VersionRange(f2, t2) = vrs2(0)
        val vr1 = vrs1(0)
        val f1 = vr1.from.v
        val t1 = vr1.to.v

        val vr2 = vrs2(0)
        val f2 = vr2.from.v
        val t2 = vr2.to.v

        if (t1 < f2) rec(vrs1.tail, vrs2, acc) // no intersection, drop left
        else if (t2 < f1) rec(vrs1, vrs2.tail, acc) // no intersection, drop right
        else { // intersection: t1 >= f2 && f1 <= t2
          val maxF = math.max(f1, f2)
          val minT = math.min(t1, t2)
          val result = acc :+ VersionRange(maxF, minT) // the resulting intersection appended to the result
          if (t1 > minT) { // there is some rest [minT,t1]
            rec(VersionRange(minT, t1) +: vrs1.tail, vrs2.tail, result)
          } else if (t2 > minT) { // there is some rest [minT,t2]
            rec(vrs1.tail, VersionRange(minT, t2) +: vrs2.tail, result)
          } else { //no rest
            rec(vrs1.tail, vrs2.tail, result)
          }
        }
    }
    new VersionRangeSet(rec(versions, vrs.versions, Vector()))
  }

  /** Computes the union of this and the given argument.
    *
    * @param vrs the VersionRangeSet to be united with this
    * @return the union of this and vrs
    */
  def union(vrs: VersionRangeSet): VersionRangeSet = {
    @tailrec
    def rec(vrss1: Vector[VersionRange], vrss2: Vector[VersionRange], acc: Vector[VersionRange]): Vector[VersionRange] = (vrss1, vrss2) match {
      case (Vector(), Vector()) => acc
      case (Vector(), vrs2)     => acc ++ vrs2
      case (vrs1, Vector())     => acc ++ vrs1
      case (vrs1, vrs2) =>
        val vr1 = vrs1(0)
        val f1 = vr1.from.v
        val t1 = vr1.to.v

        val vr2 = vrs2(0)
        val f2 = vr2.from.v
        val t2 = vr2.to.v

        if (t1 < f2) rec(vrs1.tail, vrs2, acc :+ vr1) // no intersection, keep left
        else if (t2 < f1) rec(vrs1, vrs2.tail, acc :+ vr2) // no intersection, keep right
        else { // intersection: t1 >= f2 && f1 <= t2
          val minF = math.min(f1, f2)
          val maxT = math.max(t1, t2)
          if (t1 < t2) rec(vrs1.tail, VersionRange(minF, maxT) +: vrs2.tail, acc)
          else if (t1 > t2) rec(VersionRange(minF, maxT) +: vrs1.tail, vrs2.tail, acc)
          else rec(vrs1.tail, vrs2.tail, acc :+ VersionRange(minF, maxT))
        }
    }
    VersionRangeSet.mergeRanges(rec(versions, vrs.versions, Vector()))
  }
}

object VersionRangeSet {
  /* The singleton VersionRangeSet {[Head,Head]}. */
  val HeadVersion = VersionRangeSet(Version.Head)
  /* The singleton VersionRangeSet {[0,Head]}. */
  val AllVersions = VersionRangeSet(0, Version.Head)
  /* The empty VersionRangeSet {}. */
  val Empty = new VersionRangeSet(Vector())

  /** Given a version v returns the singleton VersionRangeSet {[v,v]}
    * @param v the version
    * @return singleton VersionRangeSet {[v,v]}
    */
  def apply(v: Version): VersionRangeSet =
    VersionRangeSet(v, v)

  /** Given the versions v1 and v2 returns the singleton VersionRangeSet {[v1,v2]}
    * @param v1 the lower version
    * @param v2 the upper version
    * @return singleton VersionRangeSet {[v1,v2]}
    */
  def apply(v1: Version, v2: Version): VersionRangeSet =
    new VersionRangeSet(Vector(VersionRange(v1, v2)))

  /** Given a vector of version ranges returns the corresponding VersionRangeSet. */
  private[infrastructure] def apply(versionRanges: Vector[VersionRange]): VersionRangeSet =
    mergeRanges(versionRanges.sorted)

  private def mergeRanges(versionRanges: Vector[VersionRange]): VersionRangeSet = {
    //require(versionRanges == versionRanges.sorted, "VersionRanges must be sorted") // too expensive to check
    require(versionRanges.isEmpty || versionRanges.init.forall(_.to != Version.Head),
      "Only the greatest VersionRange is allowed to have a Head version.")

    @tailrec
    def rec(ranges: Vector[VersionRange], acc: Vector[VersionRange]): Vector[VersionRange] = ranges match {
      case Vector()   => acc
      case Vector(vr) => acc :+ vr
      case vrs => // at least size 2
        // VersionRange.unapply incurs costs of ~ 20% of query execution time, need to investigate
        // val VersionRange(from1, to1) = vrs(0)
        // val VersionRange(from2, to2) = vrs(1)
        val v1 = vrs(0)
        val from1 = v1.from.v
        val to1 = v1.to.v

        val v2 = vrs(1)
        val from2 = v2.from.v
        val to2 = v2.to.v

        if ((to1 + 1) < from2) rec(vrs.tail, acc :+ vrs(0)) // assert(to1 != Version.Head) guaranteed by require above
        else rec(VersionRange(from1, math.max(to1, to2)) +: vrs.tail.tail, acc)
    }
    val mergedRanges = rec(versionRanges, Vector.empty)
    new VersionRangeSet(mergedRanges)
  }

  def apply(vrs: VersionRange*): VersionRangeSet =
    VersionRangeSet(vrs.toVector)
}