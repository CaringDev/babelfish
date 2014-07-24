package ch.fhnw.imvs.babelfish.dsl.core

import ch.fhnw.imvs.babelfish.dsl.core.Tr._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.Identifier
import ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRangeSet._
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Versioned
import ch.fhnw.imvs.babelfish.infrastructure.{ QueryResult, QueryResultEdge, QueryResultNode }
import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaNode, SchemaElement }
import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

final case class State[+Head](
  path: List[QueryResult[Identifier]],
  cycles: Set[List[QueryResult[Identifier]]],
  labels: Map[String, (List[Any], Extractable[_])])

object State {
  val init = new State(Nil, Set(), ListMap())
}

trait TrailsPrimitives {

  final implicit class RepetitionSyntax[S, A](t1: Tr[State[S], State[S], A]) {
    def * : Tr[State[S], State[S], Stream[A]] = many(t1)
    def + : Tr[State[S], State[S], Stream[A]] = many1(t1)
  }

  final implicit class AsSyntaxImplicit[I, O, A: Extractable](val tr: Tr[State[I], State[O], A]) extends AsSyntax[I, O, A]
  // AsSyntaxNothingImplicit and AsSyntaxNothingNothingImplicit should be removed once https://issues.scala-lang.org/browse/SI-4982 is fixed.
  final implicit class AsSyntaxNothingImplicit[O, A: Extractable](val tr: Tr[State[Nothing], State[O], A]) extends AsSyntax[Nothing, O, A]
  final implicit class AsSyntaxNothingNothingImplicit[A: Extractable](val tr: Tr[State[Nothing], State[Nothing], A]) extends AsSyntax[Nothing, Nothing, A]

  abstract class AsSyntax[I, O, A: Extractable] {
    def tr: Tr[State[I], State[O], A]
    def as(name: String): Tr[State[I], State[O], A] = tr.flatMap(addLabel[O, A](name))
    def sameAs(name: String): Tr[State[I], State[O], Boolean] = tr.flatMap(wasAssigned(name))
  }

  private val defaultVersion = AllVersions

  /** All nodes of type 'sn'. */
  def V[M, N <: SchemaNode](sn: N): Tr[State[M], State[N], QueryResultNode] =
    Tr(env => in => {
      val edges = env.db.nodes(sn, defaultVersion).toStream
      edges.map(e => (in.copy(path = e :: in.path), e))
    })

  /** All edges of type 'se'. */
  def E[M, E <: SchemaEdge](se: E): Tr[State[M], State[E], QueryResultEdge] =
    Tr(env => in => {
      val edges = env.db.edges(se, true, defaultVersion).toStream
      edges.map(e => (in.copy(path = e :: in.path), e))
    })

  /** The outgoing edges of type 'se' of the current path's head node. */
  def outE[E <: SchemaEdge](se: E): Tr[State[E#From], State[E], QueryResultEdge] =
    Tr(env => in => {
      val edges = env.db.validOutgoingEdges(se, in.path.head.asInstanceOf[QueryResultNode]).toStream
      edges.map(e => (extendPath(in, e), e))
    })

  /** The incoming edges of type 'se' of the current path's head node. */
  def inE[E <: SchemaEdge](se: E): Tr[State[E#To], State[E], QueryResultEdge] =
    Tr(env => in => {
      val edges = env.db.validIncomingEdges(se, in.path.head.asInstanceOf[QueryResultNode]).toStream
      edges.map(e => (extendPath(in, e), e))
    })

  /** The source vertex of the current path's head edge. */
  def outV[E <: SchemaEdge]: Tr[State[E], State[E#From], QueryResultNode] =
    Tr(env => in => {
      val v = env.db.sourceNode(in.path.head.asInstanceOf[QueryResultEdge])
      Stream((extendPath(in, v), v))
    })

  /** The target vertex of the current path's head edge. */
  def inV[E <: SchemaEdge]: Tr[State[E], State[E#To], QueryResultNode] =
    Tr(env => in => {
      val v = env.db.targetNode(in.path.head.asInstanceOf[QueryResultEdge])
      Stream((extendPath(in, v), v))
    })

  private final def extendPath[I, O](in: State[I], el: QueryResult[Identifier]): State[O] =
    in.copy(path = el :: in.path)

  /** Retrieve the property 'p' of the current path head. */
  def get[T: ClassTag, E <: SchemaElement](p: E#SchemaProperty[T]): Tr[State[E], State[E], Versioned[T]] =
    Tr(env => in => Stream((in, env.db.propertyValues[T](p, in.path.head))))

  /** The current path. */
  def path[S]: Tr[State[S], State[S], List[QueryResult[Identifier]]] =
    getState[State[S]].map(_.path)

  def out[E <: SchemaEdge](se: E): Tr[State[E#From], State[E#To], QueryResultNode] =
    outE(se) ~> inV

  def in[E <: SchemaEdge](se: E): Tr[State[E#To], State[E#From], QueryResultNode] =
    inE(se) ~> outV

  /** Repetition (.*) */
  def many[E, A](tr: Tr[State[E], State[E], A]): Tr[State[E], State[E], Stream[A]] =
    newCycleScope(internalMany(tr))

  private final def internalMany[E, A](tr: Tr[State[E], State[E], A]): Tr[State[E], State[E], Stream[A]] =
    Tr(e => in => Tr.choice[State[E], State[E], Stream[A]](success(Stream[A]()), internalMany1(tr))(e)(in))

  /** Repetition (.+) */
  def many1[E, A](tr: Tr[State[E], State[E], A]): Tr[State[E], State[E], Stream[A]] =
    newCycleScope(internalMany1(tr))

  private def internalMany1[E, A](tr: Tr[State[E], State[E], A]): Tr[State[E], State[E], Stream[A]] =
    for {
      (sl, a) <- slice(tr)
      s @ State(_, cycles, _) <- getState[State[E]] if !cycles.contains(sl)
      _ <- setState[State[E], State[E]](s.copy(cycles = s.cycles + sl))
      as <- internalMany(tr)
    } yield a #:: as

  /** Retrieve path part generated by the traverser 'tr'. */
  private def slice[I, O, A](tr: Tr[State[I], State[O], A]): Tr[State[I], State[O], (List[QueryResult[Identifier]], A)] =
    for {
      s0 <- getState[State[I]]
      a <- tr
      s1 <- getState[State[O]]
    } yield (s1.path.take(s1.path.size - s0.path.size), a)

  private def newCycleScope[E, A](tr: Tr[State[E], State[E], A]): Tr[State[E], State[E], A] =
    for {
      s0 <- getState[State[E]]
      _ <- setState[State[E], State[E]](s0.copy(cycles = Set()))
      res <- tr
      _ <- updateState[State[E], State[E]](s1 => s1.copy(cycles = s0.cycles))
    } yield res

  def addLabel[S, A: Extractable](name: String)(a: A): Tr[State[S], State[S], A] = {
    import scala.language.existentials
    for {
      s <- getState[State[S]]
      labelTag = implicitly[Extractable[A]]
      (elems: List[Any], tag) = s.labels.get(name) match {
        case None => (Nil, labelTag)
        case Some((els, t)) =>
          if (t != labelTag) throw new IllegalStateException(s"Cannot name $a as $name (type mismatch $labelTag != $t)")
          else (els, t)
      }
      _ <- setState[State[S], State[S]](s.copy(labels = s.labels.updated(name, (a :: elems, tag))))
    } yield a
  }

  /** Retrieve list of matches for a variable 'name' on this path. */
  def getLabel[S, A](name: String): Tr[State[S], State[S], List[Any]] =
    getState.map(_.labels.get(name).map(_._1).getOrElse(Nil))

  /** Whether the slice it generates was previously assigned label 'labelName'. */
  def wasAssigned[S](labelName: String)(a: Any): Tr[State[S], State[S], Boolean] =
    for { l <- getLabel(labelName) } yield l.contains(a)
}

