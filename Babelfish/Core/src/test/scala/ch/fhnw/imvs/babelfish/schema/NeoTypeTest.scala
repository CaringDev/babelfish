package ch.fhnw.imvs.babelfish.schema

import org.scalatest.FeatureSpec

class NeoTypeTest extends FeatureSpec {
  feature("Implicit type classes"){
    scenario("Find unambiguous implicits") {
      object Node extends SchemaNode {
        object DoubleId extends SchemaProperty[Double]

        def properties: Seq[SchemaProperty[_]] = Seq(DoubleId)
        def desc: String = ""
        def id: Seq[Identity] = Seq(DoubleId)
      }
      assert(Node.DoubleId.isId) // just to reference at least once
    }
  }
}
