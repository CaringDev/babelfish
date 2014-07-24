package ch.fhnw.imvs.babelfish.infrastructure.versioning

import org.scalatest.FunSuite

/*
~test-only ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionTest
*/
class VersionTest extends FunSuite {

  def from(version: Long): Int = (version >>> 32).toInt
  def to(version: Long): Int = (version & 0x00000000FFFFFFFFl).toInt

  def from(version: Long, from: Int): Long = (version & 0x00000000FFFFFFFFl) | (from.toLong << 32)
  def to(version: Long, to: Int): Long = (version & 0xFFFFFFFF00000000l) | to

  def versionRange(from: Int, to: Int): Long = (from.toLong << 32) | to

  test("version") {
    val ver = to(from(0l, 10), 20)

    assert(10 === from(ver))
    assert(20 === to(ver))

    val ver1 = to(ver, 40)
    assert(10 === from(ver1))
    assert(40 === to(ver1))

    val ver2 = from(ver, 40)
    assert(40 === from(ver2))
    assert(20 === to(ver2))
  }

  test("version max") {
    val ver = to(from(0l, Int.MaxValue), Int.MaxValue)

    assert(Int.MaxValue === from(ver))
    assert(Int.MaxValue === to(ver))
  }

  test("versionRange") {
    val ver = versionRange(10, 20)
    assert(10 === from(ver))
    assert(20 === to(ver))
  }

  test("versionRange max") {
    val ver = versionRange(Int.MaxValue, Int.MaxValue)
    assert(Int.MaxValue === from(ver))
    assert(Int.MaxValue === to(ver))
  }

}
