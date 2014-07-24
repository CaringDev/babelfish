package ch.fhnw.imvs.babelfish.infrastructure.versioning

import org.scalatest.{ Matchers, FeatureSpec }

/*
~test-only ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionedTest
*/
class VersionedTest extends FeatureSpec with Matchers {
  feature("Single value") {
    scenario("Multiple values != single value") {
      val multiVal = Versioned(Vector((VersionRange(Version(1)), "A"), (VersionRange(Version(2)), "B"), (VersionRange(Version(4)), "C")))
      println(multiVal)

      (1 to 4).map(multiVal(_)) should be (Vector(Some("A"), Some("B"), None, Some("C")))
      multiVal.values.size should not be (1)
    }

    scenario("Single values = single value") {
      val multiVal = Versioned(Vector((VersionRange(Version(1)), "A"), (VersionRange(Version(2)), "A"), (VersionRange(Version(4)), "A")))
      (1 to 4).map(multiVal(_)) should be (Vector(Some("A"), Some("A"), None, Some("A")))
      multiVal.values.size should be (2)
      multiVal.values.distinct.size should be (1)
    }
  }
}
