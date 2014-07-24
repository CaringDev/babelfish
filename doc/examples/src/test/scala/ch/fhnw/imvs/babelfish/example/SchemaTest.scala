package ch.fhnw.imvs.babelfish.example

import org.scalatest.{ Matchers, FeatureSpec }

class SchemaTest extends FeatureSpec with Matchers {
  feature("Schema check") {
    scenario("Reference schema") {
      CarolSchema.carolsWorld should not be (null)
    }
  }
}
