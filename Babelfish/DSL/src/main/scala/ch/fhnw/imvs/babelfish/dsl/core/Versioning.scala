package ch.fhnw.imvs.babelfish.dsl.core

import ch.fhnw.imvs.babelfish.infrastructure.versioning.{ Versioned, Version, VersionRangeSet }
import ch.fhnw.imvs.babelfish.dsl.core.Tr._
import ch.fhnw.imvs.babelfish.infrastructure.{ QueryResultNode, QueryResultEdge }
import scala.language.implicitConversions
import scala.reflect.ClassTag
import ch.fhnw.imvs.babelfish.schema.neo4j.NeoType
import ch.fhnw.imvs.babelfish.schema.SchemaElement

/** Version-specific DSL functionality. */
trait Versioning { self: TrailsPrimitives =>

  /** Lift version-ignorant predicates to make them applicable in a versioned context. */
  implicit def liftPredicate[A](p: A => Boolean): (Versioned[A] => VersionRangeSet) =
    (v: Versioned[A]) => v.filter(p).versions

  final implicit class VersionedSyntax[I, O, A](tr: Tr[State[I], State[O], Versioned[A]]) {
    /** Restrict the current path to versions where the versioned property fulfills condition 'p' */
    def filterV(p: A => Boolean) = self.filterVersioned(tr)(liftPredicate(p))
  }

  /** Traverser 'tr', restricted to the versions computed by 'f'. */
  def filterVersioned[I, O, A](tr: Tr[State[I], State[O], Versioned[A]])(f: Versioned[A] => VersionRangeSet): Tr[State[I], State[O], Versioned[A]] =
    tr.flatMap { a => intersect(f(a)).map(a.limit) }

  /** The element property 'sp' iff it fulfills condition 'p'. */
  final def where[A: ClassTag: NeoType, S <: SchemaElement](sp: S#SchemaProperty[A])(p: A => Boolean): Tr[State[S], State[S], Versioned[A]] =
    filterVersioned(get[A, S](sp))(p)

  /** The versions of the current path head. */
  def versions[S]: Tr[State[S], State[S], VersionRangeSet] =
    getState[State[S]].map(_.path.head.validRanges)

  /** The version of when the current path head was first added. */
  def added[S]: Tr[State[S], State[S], Version] =
    versions.map(_.versions.head.from)

  /** The version of when the current path head was last removed. */
  def removed[S]: Tr[State[S], State[S], Version] =
    versions.map(_.versions.last.to)

  /** The version of when the current path head was first added iff it was added after version 'v'. */
  def addedAfter[S, A](v: Version): Tr[State[S], State[S], Version] =
    added.filter(_.v > v.v)

  /** The version of when the current path head was first added iff it was added before version 'v'. */
  def addedBefore[S, A](v: Version): Tr[State[S], State[S], Version] =
    added.filter(_.v < v.v)

  /** The intersection of the current result with the given VersionRangeSet.
    *
    * @param vrs The version ranges to intersect the result with.
    * @tparam S The type of the current head element.
    * @return A Traverser with the VersionRangeSet of the intersection where the head element of the path also has this VersionRangeSet.
    */
  def intersect[S](vrs: VersionRangeSet): Tr[State[S], State[S], VersionRangeSet] =
    for {
      e <- getEnv
      s <- getState[State[S]]
      head = s.path.head
      intersection = head.validRanges.intersect(vrs) if intersection.nonEmpty
      newHead = head match {
        case QueryResultEdge(id, _, dir) => e.db.queryResultEdgeFromExactRanges(id, dir, intersection)
        case QueryResultNode(id, _)      => e.db.queryResultNodeFromExactRanges(id, intersection)
      }
      _ <- setState[State[S], State[S]](s.copy(path = newHead :: s.path.tail))
    } yield intersection
}
