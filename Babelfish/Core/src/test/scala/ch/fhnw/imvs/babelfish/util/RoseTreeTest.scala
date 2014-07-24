package ch.fhnw.imvs.babelfish.util

import org.scalatest.FunSuite

/** Tests the rose tree implementation.
  * ~test-only ch.fhnw.imvs.babelfish.util.RoseTreeTest
  */
class RoseTreeTest extends FunSuite {

  val inputTree = Inner[String, Int]("ROOT", Seq(
    Leaf(1),
    Inner[String, Int]("SUB", Seq(
      Leaf(2))),
    Leaf(3)))

  val resultTree = Inner[String, (Int, String)]("ROOT", Seq(
    Leaf((1, "X")),
    Inner("SUB", Seq(
      Leaf((2, "Y")))),
    Leaf((3, "Z"))))

  test("Single tree zip test with exact number of elements to be zipped") {
    val (rest, zippedTree) = RoseTree.zipTreeWithValues(inputTree, Seq("X", "Y", "Z"))
    assert(rest === Seq())
    assert(zippedTree === resultTree)
  }

  test("Single tree zip test with remaining elements") {
    val (rest, zippedTree) = RoseTree.zipTreeWithValues(inputTree, Seq("X", "Y", "Z", "XXX"))
    assert(rest === Seq("XXX"))
    assert(zippedTree === resultTree)
  }

  test("Single tree zip test with too few elements") {
    intercept[NoSuchElementException] {
      RoseTree.zipTreeWithValues(inputTree, Seq("X", "Y"))
    }
  }

  test("Multiple trees with exact number of elements to be zipped") {
    val zippedTree = RoseTree.zipTreesWithValues(Seq(inputTree, inputTree), Seq("X", "Y", "Z", "X", "Y", "Z"))
    assert(zippedTree === Seq(resultTree, resultTree))
  }

  test("Multiple trees with remaining elements") {
    intercept[NoSuchElementException] {
      RoseTree.zipTreesWithValues(Seq(inputTree, inputTree), Seq("X", "Y", "Z", "X", "Y"))
    }
  }

  test("Multiple trees with too few elements") {
    intercept[IllegalArgumentException] {
      RoseTree.zipTreesWithValues(Seq(inputTree, inputTree), Seq("X", "Y", "Z", "X", "Y", "Z", "XXX"))
    }
  }
}
