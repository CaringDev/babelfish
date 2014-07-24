package ch.fhnw.imvs.babelfish.util

/** A tree with an arbitrary number of child nodes.
  * @tparam K type of the elems in the inner nodes
  * @tparam V type of the elems in the leaf nodes
  */
trait RoseTree[+K, +V]
case class Leaf[+V](v: V) extends RoseTree[Nothing, V]
case class Inner[+K, +V](k: K, children: Seq[RoseTree[K, V]]) extends RoseTree[K, V]

object RoseTree {
  /** Zips the leave values of the given rose tree with the given values (left to right). Remaining elements are returned
    * in the first position of the resulting tuple.
    * @param t the rose tree
    * @param lrChildValues the values to be zipped from left to right
    * @tparam K type of the elems in the inner nodes
    * @tparam V type of the elems in the leaf nodes
    * @tparam T type of the elems of {@code lrChildValues} to be zipped
    * @return a tuple containing the remaining elements and the zipped tree
    */
  def zipTreeWithValues[K, V, T](t: RoseTree[K, V], lrChildValues: Seq[T]): (Seq[T], RoseTree[K, (V, T)]) = {
    t match {
      case Leaf(v) =>
        if (lrChildValues.isEmpty) throw new NoSuchElementException()
        (lrChildValues.tail, Leaf[(V, T)]((v, lrChildValues.head)))
      case Inner(k, children) =>
        val (restValues, trees) = children.foldLeft((lrChildValues, Seq[RoseTree[K, (V, T)]]())) {
          case ((ids, res), tree) =>
            val (rest, subtree) = zipTreeWithValues(tree, ids)
            (rest, res :+ subtree)
        }
        (restValues, Inner(k, trees))
    }
  }

  /** Zips the leave values of the given rose tree sequence with the given values (left to right). It is asserted that
    * the number of leaf nodes equals the length of the sequence to be zipped.
    * @param ts a sequence of rose trees
    * @param lrChildValues the values to be zipped with the leafs of the rose trees
    * @tparam K type of the elems in the inner nodes
    * @tparam V type of the elems in the leaf nodes
    * @tparam T type of the elems of {@code lrChildValues} to be zipped
    * @return a sequence of rose trees where the leafs are zipped with the given values
    */
  def zipTreesWithValues[K, V, T](ts: Seq[RoseTree[K, V]], lrChildValues: Seq[T]): Seq[RoseTree[K, (V, T)]] = {
    val (values, trees) = ts.foldLeft((lrChildValues, Seq[RoseTree[K, (V, T)]]())) {
      case ((vals, trs), tree) =>
        val (rest, treeWithData) = zipTreeWithValues(tree, vals)
        (rest, trs :+ treeWithData)
    }
    if (values.size > 0) throw new IllegalArgumentException("Too many elements to be zipped")
    trees
  }
}