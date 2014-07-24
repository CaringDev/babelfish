package ch.fhnw.imvs.babelfish.dsl.core

import ch.fhnw.imvs.babelfish.infrastructure.{ CancelException, QueryDb }
import java.util.concurrent.atomic.AtomicBoolean
import scala.language.implicitConversions

final case class <~>[+A, +B](a: A, b: B) { override def toString: String = s"$a <~> $b" }

final class Env(val db: QueryDb)(val cancel: AtomicBoolean)

/** trails provides purely functional graph traverser combinators. */
final class Tr[I, O, +A](tr: Env => I => Stream[(O, A)]) extends (Env => I => Stream[(O, A)]) {

  def apply(e: Env): I => Stream[(O, A)] = tr(e)

  def flatMap[P, B](f: A => Tr[O, P, B]): Tr[I, P, B] =
    Tr(e => i => apply(e)(i).flatMap {
      case (m, a) =>
        if (e.cancel.get()) throw CancelException
        f(a)(e)(m)
    })

  def map[B](f: A => B): Tr[I, O, B] =
    Tr(e => i => this(e)(i).map(t => (t._1, f(t._2))))

  def filter(p: A => Boolean): Tr[I, O, A] =
    Tr(e => i => this(e)(i).filter(t => p(t._2)))

  def >>[P, B](t2: A => Tr[O, P, B]): Tr[I, P, B] = flatMap(t2)
  def <~>[P, B](t2: Tr[O, P, B]): Tr[I, P, A <~> B] = Tr.seq(this, t2)
  def ~[P, B](t2: Tr[O, P, B]): Tr[I, P, B] = ~>(t2)
  def |[B >: A](t2: => Tr[I, O, B]): Tr[I, O, B] = Tr.choice(this, t2)
  def ~>[P, B](t2: Tr[O, P, B]): Tr[I, P, B] = Tr.seq(this, t2).map{ case a <~> b => b }
  def <~[P, B](t2: Tr[O, P, B]): Tr[I, P, A] = Tr.seq(this, t2).map{ case a <~> b => a }
  def ^^[B](f: A => B): Tr[I, O, B] = map(f)

  /** Take the first 'n' paths produced by 'tr'. */
  def take(n: Int): Tr[I, O, A] =
    Tr(e => i => this(e)(i).take(n))

  def withFilter(p: A => Boolean): TrWithFilter = new TrWithFilter(this, p)
  final class TrWithFilter(tr: Tr[I, O, A], p: A => Boolean) {
    def map[B](f: A => B): Tr[I, O, B] = tr.filter(p).map(f)
    def flatMap[P, B](f: A => Tr[O, P, B]): Tr[I, P, B] = tr.filter(p).flatMap(f)
    def withFilter(q: A => Boolean): TrWithFilter = new TrWithFilter(tr, x => p(x) && q(x))
  }
}

object Tr {

  def apply[I, O, A](t1: Env => I => Stream[(O, A)]): Tr[I, O, A] = new Tr(t1)

  final implicit class OptSyntax[S, A](tr: Tr[S, S, A]) {
    def ? : Tr[S, S, Option[A]] = opt(tr)
  }

  /** Returns the sequential composition of traversers t1 and t2.
    * @return the sequential composition of traversers t1 and t2
    */
  def seq[I, O, P, A, B](t1: Tr[I, O, A], t2: Tr[O, P, B]): Tr[I, P, A <~> B] =
    for (a <- t1; b <- t2) yield new <~>(a, b)

  /** Returns the 'parallel' composition of traversers t1 and t2.
    * @return the parallel composition of traversers t1 and t2
    */
  def choice[I, O, A](t1: Tr[I, O, A], t2: => Tr[I, O, A]): Tr[I, O, A] =
    Tr(e => i => t1(e)(i) #::: t2(e)(i))

  /** Returns a traverser which optionally follows tr.
    * @return a traverser which optionally follows tr
    */
  def opt[S, A](tr: Tr[S, S, A]): Tr[S, S, Option[A]] =
    Tr(e => i => choice(success[S, Option[A]](None), tr.map(Some(_)))(e)(i))

  def filter[S, A](p: A => Boolean): (A) => Tr[S, S, A] =
    a => if (p(a)) success(a) else fail

  /** Returns a traverser which returns its input as the output.
    * @return a traverser which returns its input
    */
  def success[S, A](a: A): Tr[S, S, A] =
    Tr(_ => s => Stream((s, a)))

  /** Returns a traverser which drops its input and returns an empty output.
    * @return a traverser which drops its input
    */
  def fail[S]: Tr[S, S, Nothing] =
    Tr(_ => _ => Stream())

  def getEnv[S]: Tr[S, S, Env] =
    Tr(e => s => Stream((s, e)))

  def getState[S]: Tr[S, S, S] =
    Tr(_ => s => Stream((s, s)))

  def setState[I, O](o: O): Tr[I, O, Unit] =
    Tr(_ => _ => Stream((o, ())))

  def updateState[I, O](f: I => O): Tr[I, O, Unit] =
    getState[I] flatMap (i => setState(f(i)))

  /** Execute sub-query 'tr', continue with its result. */
  def sub[S, O, A](tr: Tr[S, O, A]): Tr[S, S, Stream[A]] =
    Tr(e => s => Stream((s, tr(e)(s).map(_._2))))

  /** Whether the paths matches the given 'pattern'. */
  def matches[S, E, A](pattern: Tr[S, E, A]): Tr[S, S, Boolean] =
    sub(pattern).map(_.nonEmpty)
}

