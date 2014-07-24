package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.importer.EdgeData
import ch.fhnw.imvs.babelfish.importer.NodeData
import ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRangeSet
import ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRangeSet._
import ch.fhnw.imvs.babelfish.infrastructure.{ QueryResultNode, QueryResultEdge, QueryResult }
import ch.fhnw.imvs.babelfish.schema._
import org.scalatest.{ Matchers, FeatureSpec, OptionValues }
import scala.reflect.ClassTag

/*
~test-only ch.fhnw.imvs.babelfish.infrastructure.lowlevel.DbReadTest
*/
class DbReadTest extends FeatureSpec with OptionValues with Matchers {

  import scala.language.implicitConversions
  import scala.language.reflectiveCalls

  implicit def qr2prop(qrn: QueryResult[Identifier]) = new {
    def propertyValue[T: ClassTag](db: TestReadWriteDb, sp: SchemaElement#SchemaProperty[T]): (VersionRangeSet, T) = {
      val props = db.propertyValues(sp, qrn)
      assert(props.values.size == 1)
      props.extract.head
    }
  }

  object oneVersionSchema extends Schema("SimpleSchema") {
    object A extends SchemaNode {
      def desc = ""
      object A_id_prop_0 extends SchemaProperty[Int]
      object A_data_prop_0 extends SchemaProperty[String]

      def id = Seq(A_id_prop_0)
      val properties: Seq[SchemaProperty[_]] = Seq(A_id_prop_0, A_data_prop_0)
    }

    object B extends SchemaNode {
      def desc = ""
      object B_id_prop_0 extends SchemaProperty[Int]
      object B_data_prop_0 extends SchemaProperty[String]

      def id = Seq(B_id_prop_0)
      val properties: Seq[SchemaProperty[_]] = Seq(B_id_prop_0, B_data_prop_0)
    }

    object AtoB extends SchemaEdge {
      def desc = ""
      type From = A.type; type To = B.type
      def from = A; def to = B

      object Edge_AtoB_prop extends SchemaProperty[String]
      val properties: Seq[SchemaProperty[_]] = Seq(Edge_AtoB_prop)
    }

    object BtoB extends SchemaEdge {
      def desc = ""
      type From = B.type; type To = B.type
      def from = B; def to = B

      object Edge_BtoB_prop extends SchemaProperty[String]
      val properties: Seq[SchemaProperty[_]] = Seq(Edge_BtoB_prop)
    }

    def nodes: Seq[SchemaNode] = Seq(A, B)
    def edges: Seq[SchemaEdge] = Seq(BtoB, AtoB)
  }

  /*
     A     B    B
     1 <-> 1
      ^
        \
          V
     2 <-> 2 -> 2      //self$ Ref
     3 <-> 3 -> 1
      ^
        \
          V
     4 <-> 4 -> 2      //self$ Ref
  */

  def oneVersionFill(db: TestReadWriteDb) {

    import oneVersionSchema._

    val nodes = Seq(
      (A, NodeData(Seq(1), Seq("Hi1"))),
      (A, NodeData(Seq(2), Seq("Hi2"))),
      (A, NodeData(Seq(3), Seq("Hi3"))),
      (A, NodeData(Seq(4), Seq("Hi4"))),
      (B, NodeData(Seq(1), Seq("Hi1"))),
      (B, NodeData(Seq(2), Seq("Hi2"))),
      (B, NodeData(Seq(3), Seq("Hi3"))),
      (B, NodeData(Seq(4), Seq("Hi4"))))

    val edges = Seq(
      (AtoB, EdgeData(Seq(1), Seq(1), Seq("e1"))),
      (AtoB, EdgeData(Seq(1), Seq(2), Seq("e2"))),
      (AtoB, EdgeData(Seq(2), Seq(2), Seq("e3"))),
      (AtoB, EdgeData(Seq(3), Seq(3), Seq("e4"))),
      (AtoB, EdgeData(Seq(3), Seq(4), Seq("e5"))),
      (AtoB, EdgeData(Seq(4), Seq(4), Seq("e6"))),
      (BtoB, EdgeData(Seq(2), Seq(2), Seq("e1"))),
      (BtoB, EdgeData(Seq(3), Seq(1), Seq("e2"))),
      (BtoB, EdgeData(Seq(4), Seq(4), Seq("e3"))))

    nodes.foreach{ case (sn, nd) => db.createOrUpdateNode(sn, nd) }
    edges.foreach{ case (se, ed) => db.createOrUpdateEdge(se, ed) }
  }

  feature("everything..."){
    scenario("Unique identification") {

      DbTest.withDb[Unit](oneVersionSchema){ db =>
        oneVersionFill(db)

        import oneVersionSchema._

        def allNodesAreFound(schemaNode: SchemaNode) {
          val allA = db.nodes(schemaNode, AllVersions)
          val allUniqueIdsA = allA.map(_.id.numericId)
          val allFromUniqueIdsA = allUniqueIdsA.map { id =>
            val idNode = db.findNodeById(id).value
            QueryResultNode(idNode, db.versions(idNode))
          }
          allA should be (allFromUniqueIdsA)
        }

        allNodesAreFound(A)
        allNodesAreFound(B)

        def allEdgesAreFound(schemaEdge: SchemaEdge) {
          val allAtoB = db.edges(schemaEdge, true, AllVersions)
          val allUniqueIdsAtoB = allAtoB.map(_.numericId)
          val allFromUniqueIdsAtoB = allUniqueIdsAtoB.map { id =>
            val edge = db.findEdgeById(id).value
            QueryResultEdge(edge, db.versions(edge), true)
          }
          allAtoB should be (allFromUniqueIdsAtoB)
        }

        allEdgesAreFound(AtoB)
        allEdgesAreFound(BtoB)

      }
    }
  }
}