package ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index

import ch.fhnw.imvs.babelfish.importer.NodeData
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.DbTest
import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaNode, Schema }
import org.scalatest.{ Matchers, OptionValues, FeatureSpec }

/*
~test-only ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.IndexAPITest
*/
class IndexAPITest extends FeatureSpec with OptionValues with Matchers {

  object s0 extends Schema("S") {
    object N extends SchemaNode {
      def desc = ""
      object Prop extends SchemaProperty[String]

      def id = Seq(Prop)
      val properties: Seq[SchemaProperty[_]] = Seq(Prop)
    }
    def edges: Seq[SchemaEdge] = Seq()
    def nodes: Seq[SchemaNode] = Seq(N)
  }

  object s1 extends Schema("S") {
    object N extends SchemaNode {
      def desc = ""
      object ArrProp extends SchemaProperty[Vector[String]]

      def id = Seq(ArrProp)
      val properties: Seq[SchemaProperty[_]] = Seq(ArrProp)
    }
    def edges: Seq[SchemaEdge] = Seq()
    def nodes: Seq[SchemaNode] = Seq(N)
  }

  object optionSchema extends Schema("S") {
    object N extends SchemaNode {
      def desc = ""
      object OptProp extends SchemaProperty[Option[String]]

      def id = Seq(OptProp)
      val properties: Seq[SchemaProperty[_]] = Seq(OptProp)
    }

    def edges: Seq[SchemaEdge] = Seq()
    def nodes: Seq[SchemaNode] = Seq(N)
  }

  feature("All"){
    scenario("Simple Index Lookup") {
      val N = s0.node("N").value
      DbTest.withDb(s0){ db =>
        val node = db.createOrUpdateNode(N, NodeData(Seq("A"), Seq())).value
        db.findNodeByFullId(N, Seq("A")).value should be (node)
      }
    }

    scenario("ArrayPropIndex") {
      val N = s1.node("N").value
      DbTest.withDb(s1) { db =>
        val node = db.createOrUpdateNode(N, NodeData(Seq(Vector("A", "B")), Seq())).value
        db.findNodeByFullId(N, Seq(Vector("A", "B"))).value should be (node)
      }
    }

    scenario("SpecialChar") {
      val N = s0.node("N").value
      DbTest.withDb(s0) { db =>
        val node = db.createOrUpdateNode(N, NodeData(Seq("*"), Seq())).value
        db.findNodeByFullId(N, Seq("*")).value should be (node)
      }
    }

    scenario("OptionPropIndex") {
      DbTest.withDb(optionSchema) ({ db =>
        val N = db.schema.node("N").value
        val node = db.createOrUpdateNode(N, NodeData(Seq(Some("A")), Seq())).value
        db.findNodeByFullId(N, Seq(Some("A"))).value should be (node)
      })
    }
  }
}
