package ch.fhnw.imvs.babelfish.infrastructure.versioning

/** Allow ordering of versioned values chronologically or by value(s) */
trait VersionedOrdering {
  def byFrom[T]: Ordering[Versioned[T]] =
    by(_.headOption.map(_._1.from.v))

  def byTo[T]: Ordering[Versioned[T]] =
    by(_.lastOption.map(_._1.to.v))

  def byOldestValue[T](implicit order: Ordering[T]): Ordering[Versioned[T]] =
    by(_.headOption.map(_._2))

  def byNewestValue[T](implicit order: Ordering[T]): Ordering[Versioned[T]] =
    by(_.lastOption.map(_._2))

  def byMinValue[T](implicit order: Ordering[T]): Ordering[Versioned[T]] =
    by[T, T](x => x.map(_._2).reduceOption(order.min))

  def byMaxValue[T](implicit order: Ordering[T]): Ordering[Versioned[T]] =
    by[T, T](x => x.map(_._2).reduceOption(order.max))

  private def by[T, V](extr: Vector[(VersionRange, T)] => Option[V])(implicit order: Ordering[V]) = new Ordering[Versioned[T]] {
    def compare(x: Versioned[T], y: Versioned[T]): Int = {
      (extr(x.data), extr(y.data)) match {
        case (None, None)       => 0
        case (None, Some(_))    => -1
        case (Some(_), None)    => 1
        case (Some(a), Some(b)) => order.compare(a, b)
      }
    }
  }
}
