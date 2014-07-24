package ch.fhnw.imvs.babelfish

import scala.collection.{ Traversable, TraversableLike }

/** Provides various (widely used) utility classes not scoped to a specific task. */
package object util {
  implicit class Individualize[A](many: TraversableLike[A, Traversable[A]]) {
    def single: A = {
      assert(many.size == 1, s"Single value expected, got $many")
      many.head
    }

    def singleOption: Option[A] = {
      assert(many.size <= 1, s"None or single value expected, got $many")
      many.headOption
    }
  }
}
