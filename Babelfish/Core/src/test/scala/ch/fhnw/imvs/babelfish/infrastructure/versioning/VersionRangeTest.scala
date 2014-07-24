package ch.fhnw.imvs.babelfish.infrastructure.versioning

import org.scalatest.{ Matchers, FunSuite }

/*
 ~test-only ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRangeTest
 */
class VersionRangeTest extends FunSuite with Matchers {

  test("versionranges equality test") {
    VersionRange(2, 7) should not be (VersionRange(12, 15))
    VersionRange(2, 7) should not be (VersionRange(2, 9))
    VersionRange(2, 7) should not be (VersionRange(1, 7))
    VersionRange(2, 7) should not be (VersionRange(7, 12))
    VersionRange(2, 7) should not be (VersionRange(8, 12))
    VersionRange(2, 7) should not be (VersionRange(0, 2))
    VersionRange(2, 7) should not be (VersionRange(0, 1))
    VersionRange(2, 7) should be(VersionRange(2, 7))

    VersionRangeSet() should be('empty)
    VersionRangeSet(VersionRange(2, 15)) should be(VersionRangeSet(VersionRange(2, 15)))
    VersionRangeSet(VersionRange(2, 15), VersionRange(16, 20)) should be(VersionRangeSet(VersionRange(2, 20)))
  }

  test("versionranges merging test") {
    val v1 = VersionRangeSet(VersionRange(2, 15))
    val v2 = VersionRangeSet(VersionRange(2, 15))
    val vv1 = VersionRangeSet(VersionRange(2, 7), VersionRange(8, 15))
    val vv2 = VersionRangeSet(VersionRange(2, 12), VersionRange(8, 15))
    val vv3 = VersionRangeSet(VersionRange(2, 7), VersionRange(5, 15))

    val vvv1 = VersionRangeSet(VersionRange(2, 7), VersionRange(8, 16), VersionRange(17, 20))
    val vvv2 = VersionRangeSet(VersionRange(2, 7), VersionRange(8, 15), VersionRange(17, 20))
    val vvv3 = VersionRangeSet(VersionRange(2, 7), VersionRange(8, 15), VersionRange(17, 20), VersionRange(21, 30))

    v1 should be(v2)
    v1 should be(vv1)
    v1 should be(vv2)
    v1 should be(vv3)

    vvv1 should be(VersionRangeSet(VersionRange(2, 20)))
    vvv2 should be(VersionRangeSet(VersionRange(2, 15), VersionRange(17, 20)))
    vvv3 should be(VersionRangeSet(VersionRange(2, 15), VersionRange(17, 30)))
  }

  test("versionranges sorting test") {
    val vvv1 = VersionRangeSet(VersionRange(2, 7), VersionRange(8, 16), VersionRange(17, 20))
    val vvv2 = VersionRangeSet(VersionRange(17, 20), VersionRange(2, 7), VersionRange(8, 16))
    val vvv3 = VersionRangeSet(VersionRange(17, 20), VersionRange(2, 7), VersionRange(9, 15))
    val vvv4 = VersionRangeSet(VersionRange(17, 20), VersionRange(2, 7), VersionRange(9, 16))
    vvv1 should be(vvv2)
    vvv3.versions.toList should be(VersionRange(2, 7) :: VersionRange(9, 15) :: VersionRange(17, 20) :: Nil)
    vvv4.versions.toList should be(VersionRange(2, 7) :: VersionRange(9, 20) :: Nil)
  }

  test("versionranges intersection containing test") {
    val v1 = VersionRangeSet(VersionRange(2, 7))
    val v2 = VersionRangeSet(VersionRange(4, 7))
    val v3 = VersionRangeSet(VersionRange(4, 5))
    val v4 = VersionRangeSet(VersionRange(2, 5))

    v1.intersect(v1) should be(v1)
    v1.intersect(v2) should be(v2)
    v1.intersect(v3) should be(v3)
    v1.intersect(v4) should be(v4)

    val vv1 = VersionRangeSet(VersionRange(2, 7), VersionRange(12, 15))
    val vv2 = VersionRangeSet(VersionRange(3, 6), VersionRange(13, 14))
    val vv3 = VersionRangeSet(VersionRange(4, 7), VersionRange(14, 15))
    val vv4 = VersionRangeSet(VersionRange(2, 5), VersionRange(12, 14))
    val vv5 = VersionRangeSet(VersionRange(2, 5), VersionRange(13, 15))

    vv1.intersect(vv1) should be(vv1)
    vv1.intersect(vv2) should be(vv2)
    vv1.intersect(vv3) should be(vv3)
    vv1.intersect(vv4) should be(vv4)
    vv1.intersect(vv5) should be(vv5)
  }

  test("versionranges intersection overlapping test") {
    val vv1 = VersionRangeSet(VersionRange(2, 7), VersionRange(12, 15))
    val vv2 = VersionRangeSet(VersionRange(5, 10), VersionRange(14, 20))
    val vv3 = VersionRangeSet(VersionRange(0, 5), VersionRange(9, 13))
    val vv4 = VersionRangeSet(VersionRange(0, 9), VersionRange(11, 16))
    val vv5 = VersionRangeSet(VersionRange(4, 13))

    vv1.intersect(vv1) should be(vv1)
    vv1.intersect(vv2) should be(VersionRangeSet(VersionRange(5, 7), VersionRange(14, 15)))
    vv1.intersect(vv3) should be(VersionRangeSet(VersionRange(2, 5), VersionRange(12, 13)))
    vv1.intersect(vv4) should be(VersionRangeSet(VersionRange(2, 7), VersionRange(12, 15)))
    vv1.intersect(vv5) should be(VersionRangeSet(VersionRange(4, 7), VersionRange(12, 13)))
  }

  test("versionranges intersection mixed test") {
    val vv1 = VersionRangeSet(VersionRange(2, 7), VersionRange(12, 15))
    val vv2 = VersionRangeSet(VersionRange(5, 10), VersionRange(13, 14))
    val vv3 = VersionRangeSet(VersionRange(0, 9), VersionRange(14, 17))

    vv1.intersect(vv1) should be(vv1)
    vv1.intersect(vv2) should be(VersionRangeSet(VersionRange(5, 7), VersionRange(13, 14)))
    vv1.intersect(vv3) should be(VersionRangeSet(VersionRange(2, 7), VersionRange(14, 15)))
  }

  test("versionranges intersection reducing test") {
    val v1 = VersionRangeSet(VersionRange(10, 20))
    val vv1 = VersionRangeSet(VersionRange(5, 6), VersionRange(12, 15), VersionRange(21, 30))
    val vv2 = VersionRangeSet(VersionRange(12, 15), VersionRange(21, 30))
    val vv3 = VersionRangeSet(VersionRange(5, 6), VersionRange(12, 15))
    val vv4 = VersionRangeSet(VersionRange(5, 6), VersionRange(8, 15), VersionRange(21, 30))

    v1.intersect(vv1) should be(VersionRangeSet(VersionRange(12, 15)))
    v1.intersect(vv2) should be(VersionRangeSet(VersionRange(12, 15)))
    v1.intersect(vv3) should be(VersionRangeSet(VersionRange(12, 15)))
    v1.intersect(vv4) should be(VersionRangeSet(VersionRange(10, 15)))
  }

  test("versionrangeset union test") {
    VersionRangeSet.Empty.union(VersionRangeSet.Empty) should be(VersionRangeSet.Empty)
    val v1 = VersionRangeSet(VersionRange(10, 20))
    v1.union(VersionRangeSet.Empty) should be(v1)
    VersionRangeSet.Empty.union(v1) should be(v1)

    val v2 = VersionRangeSet(VersionRange(10, 20), VersionRange(30, 40))
    val v3 = VersionRangeSet(VersionRange(10, 29))
    v2.union(v3) should be(VersionRangeSet(VersionRange(10, 40)))

    val v4 = VersionRangeSet(VersionRange(21, 30))
    v1.union(v4) should be(VersionRangeSet(VersionRange(10, 30)))

    VersionRangeSet(VersionRange(1, 5)).union(VersionRangeSet(VersionRange(3, 7))) should be(VersionRangeSet(VersionRange(1, 7)))
    VersionRangeSet(VersionRange(3, 7)).union(VersionRangeSet(VersionRange(1, 5))) should be(VersionRangeSet(VersionRange(1, 7)))

    VersionRangeSet(VersionRange(1, 5)).union(VersionRangeSet(VersionRange(1, 7), VersionRange(9, 9))) should be(VersionRangeSet(VersionRange(1, 7), VersionRange(9, 9)))

    VersionRangeSet.HeadVersion.union(VersionRangeSet.HeadVersion) should be(VersionRangeSet.HeadVersion)

    VersionRangeSet(VersionRange(1, 4), VersionRange(7, Version.Head)).union(VersionRangeSet(VersionRange(5, Version.Head))) should be(VersionRangeSet(VersionRange(1, Version.Head)))

    VersionRangeSet(VersionRange(1, Version.Head)).union(VersionRangeSet(VersionRange(5, Version.Head))) should be(VersionRangeSet(VersionRange(1, Version.Head)))
  }
}
