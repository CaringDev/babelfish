package ch.fhnw.imvs.babelfish.infrastructure.versioning

import scala.annotation.tailrec
import scala.reflect.ClassTag

/** Abstraction for data which has different values in different version ranges. */
class Versioned[+T] private (
  // sorted (chronologically ascending), distinct version ranges
  val data: Vector[(VersionRange, T)]) extends (Version => Option[T]) {

  /** All values (not distinct). */
  def values: Vector[T] = data.map(_._2)
  /** The union of all version ranges. */
  def versions: VersionRangeSet =
    VersionRangeSet(data.map(_._1))

  override def hashCode() = data.hashCode()
  override def equals(obj: Any) = obj match {
    case v: Versioned[T] => data == v.data
    case _               => false
  }

  override def toString() =
    data.map{ case (vr, value) => s"${vr.from} to ${vr.to} => $value" }.mkString("Versioned(", ", ", ")")

  /** Returns the value at version v which may be not defined - thus Option[T] */
  def apply(v: Version): Option[T] =
    data.collectFirst{ case (vr, value) if vr.contains(v) => value }

  /** Returns a Versioned containing only those values for which the predicate p holds. */
  def filter(p: T => Boolean): Versioned[T] =
    new Versioned(data.filter(e => p(e._2)))

  /** Split this Versioned[_] into a Seq of Versioned[_] with only one VersionRange each. */
  def split: Seq[Versioned[T]] =
    extract.flatMap { case (vrs, t) => vrs.versions.map(Versioned(_, t)) }

  /** Every distinct value of type T is returned together with its ranges of validity (VersionRangeSet).  */
  def extract: Seq[(VersionRangeSet, T)] =
    data.groupBy(_._2).map{ case (v, vals) => (VersionRangeSet(vals.map(_._1)), v) }.toSeq

  /** Limits (intersects) this Versioned by the given versions. */
  def limit(versions: VersionRangeSet): Versioned[T] = {

    @tailrec
    def rec(vrss1: Vector[(VersionRange, T)], vrss2: Vector[VersionRange], acc: Vector[(VersionRange, T)]): Vector[(VersionRange, T)] = (vrss1, vrss2) match {
      case (Vector(), _) => acc
      case (_, Vector()) => acc
      case (vrs1, vrs2) =>
        // VersionRange.unapply incurs costs of ~ 20% of query execution time, need to investigate
        val (vr1, a) = vrs1(0)
        val f1 = vr1.from.v
        val t1 = vr1.to.v

        val vr2 = vrs2(0)
        val f2 = vr2.from.v
        val t2 = vr2.to.v

        if (t1 < f2) rec(vrs1.tail, vrss2, acc) // no intersection, drop left
        else if (t2 < f1) rec(vrss1, vrs2.tail, acc) // no intersection, drop right
        else { // intersection: t1 >= f2 && f1 <= t2
          val maxF = math.max(f1, f2)
          val minT = math.min(t1, t2)
          val intersection = acc :+ ((VersionRange(maxF, minT), a))
          if (t1 > minT) {
            rec((VersionRange(minT, t1), a) +: vrs1.tail, vrs2.tail, intersection)
          } else if (t2 > minT) {
            rec(vrs1.tail, VersionRange(minT, t2) +: vrs2.tail, intersection)
          } else { //no rest
            rec(vrs1.tail, vrs2.tail, intersection)
          }
        }
    }
    new Versioned(rec(data, versions.versions, Vector()))
  }
}

object Versioned {
  def apply(): Versioned[Nothing] =
    new Versioned[Nothing](Vector())

  def apply[T](vs: VersionRange, value: T): Versioned[T] =
    new Versioned[T](Vector((vs, value)))

  def apply[T](vs: VersionRangeSet, value: T): Versioned[T] =
    new Versioned[T](vs.versions.map((_, value)))

  /** Given a vector of pairs where each pair associates a value with a VersionRange a corresponding Versioned is created. */
  private def seqApply[T](entryArr: Vector[(VersionRange, T)]): Versioned[T] = {
    //pre: version ranges are non overlapping but may be adjacent and may need to be fused
    val sortedVersionRanges = entryArr.sortBy(_._1.from.v)
    require(sortedVersionRanges.isEmpty || sortedVersionRanges.init.forall(_._1.to != Version.Head))

    // Adjacent ranges are merged but only if they have the same associated value.
    @tailrec
    def mergeRanges(ranges: Vector[(VersionRange, T)], acc: Vector[(VersionRange, T)]): Vector[(VersionRange, T)] = ranges match {
      case Vector()   => acc
      case Vector(vr) => acc :+ vr
      case vrs => // at least size 2
        // VersionRange.unapply incurs costs of ~ 20% of query execution time, need to investigate
        val (vr1, a1) = vrs(0)
        val f1 = vr1.from.v
        val t1 = vr1.to.v

        val (vr2, a2) = vrs(1)
        val f2 = vr2.from.v
        val t2 = vr2.to.v

        // if the values are different or ranges are disjoint: no merge
        if ((a1 != a2) || (t1 + 1) < f2) mergeRanges(vrs.tail, acc :+ vrs(0)) // assert(to1 != Version.Head) guaranteed by assert above
        else mergeRanges((VersionRange(f1, math.max(t1, t2)), a1) +: vrs.tail.tail, acc)
    }

    new Versioned[T](mergeRanges(sortedVersionRanges, Vector()))
  }

  def apply[T](entries: Vector[(VersionRange, T)]): Versioned[T] =
    entries.size match {
      case 0 => apply()
      case 1 => apply(entries.head._1, entries.head._2)
      case _ => seqApply(entries)
    }
}
