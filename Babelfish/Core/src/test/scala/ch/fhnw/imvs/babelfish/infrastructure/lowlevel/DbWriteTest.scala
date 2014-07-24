package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.importer.{ CsvSchema, EdgeData, CsvImporter, NodeData }
import ch.fhnw.imvs.babelfish.infrastructure.QueryResultEdge
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.DbTest._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.SchemaMappingDefinitions.PROPERTY_EDGE_TYPE
import ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRangeSet._
import ch.fhnw.imvs.babelfish.infrastructure.versioning.{ VersionRangeSet, Version, VersionRange }
import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, Schema, SchemaNode }
import ch.fhnw.imvs.babelfish.util._
import org.neo4j.graphdb.{ DynamicRelationshipType, Direction }
import org.scalatest.{ Matchers, FeatureSpec, OptionValues }

/*
~test-only ch.fhnw.imvs.babelfish.infrastructure.lowlevel.DbWriteTest
*/
class DbWriteTest extends FeatureSpec with Matchers with OptionValues {

  object simpleSchema extends Schema("SimpleSchema") {
    object A extends SchemaNode {
      def desc = ""
      object A_id_prop_0 extends SchemaProperty[String]
      object A_data_prop_0 extends SchemaProperty[String]

      def id = Seq(A_id_prop_0)
      val properties: Seq[SchemaProperty[_]] = Seq(A_id_prop_0, A_data_prop_0)
    }

    def edges: Seq[SchemaEdge] = Seq()
    def nodes: Seq[SchemaNode] = Seq(A)
  }

  object singleIdRefSchema extends Schema("SimpleSchema") {
    object B extends SchemaNode {
      def desc = ""
      object B_id_prop_0 extends SchemaProperty[String]

      def id = Seq(B_id_prop_0)
      val properties: Seq[SchemaProperty[_]] = Seq(B_id_prop_0)
    }

    object A extends SchemaNode {
      def desc = ""
      object A_id_prop_0 extends SchemaProperty[String]
      object A_data_prop_0 extends SchemaProperty[String]

      def id = Seq(A_id_prop_0, A_id_ref0)
      val properties: Seq[SchemaProperty[_]] = Seq(A_id_prop_0, A_data_prop_0)
    }

    object A_id_ref0 extends SchemaEdge {
      def desc = ""
      type From = A.type; type To = B.type
      def from = A; def to = B
      val properties: Seq[SchemaProperty[_]] = Seq()
    }

    def edges: Seq[SchemaEdge] = Seq(A_id_ref0)
    def nodes: Seq[SchemaNode] = Seq(B, A)
  }

  object doubleIdRefSchema extends Schema("SimpleSchema") {

    object C extends SchemaNode {
      def desc = ""
      object C_id_prop_0 extends SchemaProperty[String]

      def id = Seq(C_id_prop_0)
      val properties: Seq[SchemaProperty[_]] = Seq(C_id_prop_0)
    }

    object B extends SchemaNode {
      def desc = ""
      object B_id_prop_0 extends SchemaProperty[String]

      def id = Seq(B_id_prop_0, B_id_ref0)
      val properties: Seq[SchemaProperty[_]] = Seq(B_id_prop_0)
    }

    object B_id_ref0 extends SchemaEdge {
      def desc = ""
      type From = B.type; type To = C.type
      def from = B; def to = C
      val properties: Seq[SchemaProperty[_]] = Seq()
    }

    object A extends SchemaNode {
      def desc = ""
      object A_id_prop_0 extends SchemaProperty[String]
      object A_data_prop_0 extends SchemaProperty[String]

      def id = Seq(A_id_prop_0, A_id_ref0)
      val properties: Seq[SchemaProperty[_]] = Seq(A_id_prop_0, A_data_prop_0)
    }

    object A_id_ref0 extends SchemaEdge {
      def desc = ""
      type From = A.type; type To = B.type
      def from = A; def to = B
      val properties: Seq[SchemaProperty[_]] = Seq()
    }

    def edges: Seq[SchemaEdge] = Seq(A_id_ref0, B_id_ref0)
    def nodes: Seq[SchemaNode] = Seq(C, B, A)
  }

  object simpleEdgeSchema extends Schema("SimpleSchema") {
    object A extends SchemaNode {
      def desc = ""
      object A_id_prop_0 extends SchemaProperty[String]
      def id = Seq(A_id_prop_0)
      val properties: Seq[SchemaProperty[_]] = Seq(A_id_prop_0)
    }

    object B extends SchemaNode {
      def desc = ""
      object B_id_prop_0 extends SchemaProperty[String]

      def id = Seq(B_id_prop_0)
      val properties: Seq[SchemaProperty[_]] = Seq(B_id_prop_0)
    }

    object AB extends SchemaEdge {
      def desc = ""
      type From = A.type; type To = B.type
      def from = A; def to = B

      object AB_prop_0 extends SchemaProperty[String]
      val properties: Seq[SchemaProperty[_]] = Seq(AB_prop_0)
    }

    def edges: Seq[SchemaEdge] = Seq(AB)
    def nodes: Seq[SchemaNode] = Seq(A, B)
  }

  feature("Schema can be stored in db and loaded again.") {
    scenario("Simple Schema storing and loading"){
      DbTest.withDb(simpleSchema){
        _.schema.node("A").value.name should be ("A")
      }
    }
  }

  feature("Created nodes can be found via FullIdIndex.") {

    scenario("Single node is inserted and queried."){
      DbTest.withDb(simpleSchema){ db =>
        import simpleSchema._
        val node = db.createOrUpdateNode(A, NodeData(Seq("id0"), Seq("val0"))).value
        db.findNodeByFullId(A, Seq("id0")) should be (Some(node))
      }
    }

    scenario("Find in id index") {
      DbTest.withDb(singleIdRefSchema){ db =>
        import singleIdRefSchema._

        val ID_EDGE = A_id_ref0

        val idB = db.createOrUpdateNode(B, NodeData(Seq("bid0"), Seq())).value
        val idA = db.createOrUpdateNode(A, NodeData(Seq("aid0", "bid0"), Seq("val0"))).value

        db.findNodeByFullId(A, Seq("aid0", "bid0")) should be (Some(idA))
        db.findNodeByFullId(B, Seq("bid0")) should be (Some(idB))
        idA.dbNode.hasRelationship(DynamicRelationshipType.withName(ID_EDGE.name), Direction.OUTGOING) should be (true)
      }
    }
  }

  feature("Data from CSV can be imported.") {
    scenario("Simple"){
      val BNodeContent =
        """
          |B_id_prop_0
          |"bid"
        """.stripMargin

      val ANodeContent =
        """
          |A_id_prop_0,A_id_ref0>B#B_id_prop_0,A_data_prop_0
          |"aid","bid","adata"
        """.stripMargin

      DbTest.withDb(singleIdRefSchema) { db =>
        import singleIdRefSchema._
        val (bHeader, bValues) = CsvImporter.parseSource(BNodeContent).value
        val csvSchema = CsvSchema.explainSchema(db.schema).mapValues(_._2)
        val bData = CsvImporter.readNodes(bHeader, bValues.zipWithIndex, B, csvSchema(B)).head
        val (aHeader, aValues) = CsvImporter.parseSource(ANodeContent).value
        val aData = CsvImporter.readNodes(aHeader, aValues.zipWithIndex, A, csvSchema(A)).head

        db.createOrUpdateNode(B, bData)
        val neoNodeA = db.createOrUpdateNode(A, aData).value.dbNode
        val propEdge = neoNodeA.getSingleRelationship(PROPERTY_EDGE_TYPE, Direction.OUTGOING)

        val a = db.findNodeByFullId(A, Seq("aid", "bid")).value.dbNode
        val validPropEdges = db.propertyEdges(a).filter(e => db.versionRange(e).contains(1))

        validPropEdges should contain (propEdge)
      }
    }
  }

  feature("Data can be updated."){

    scenario("Update without modification") {
      withDb(simpleSchema) { db =>
        import simpleSchema._

        val Adata = A.A_data_prop_0
        val a1 = db.createOrUpdateNode(A, NodeData(Seq("aid"), Seq("aval1"))).value

        val dbV2 = db.inNextVersion()
        val a2 = dbV2.createOrUpdateNode(A, NodeData(Seq("aid"), Seq("aval1"))).value

        a1.dbNode should be (a2.dbNode)
        dbV2.nodes(A, AllVersions) should have size (1)

      }
    }

    scenario("insert edge") {
      withDb(simpleEdgeSchema) { db =>
        import simpleEdgeSchema._
        val ABP = AB.AB_prop_0

        db.createOrUpdateNode(A, NodeData(Seq("aid"), Seq()))
        db.createOrUpdateNode(B, NodeData(Seq("bid"), Seq()))
        val abEdge = db.createOrUpdateEdge(AB, EdgeData(Seq("aid"), Seq("bid"), Seq("data1"))).value

        val prop = db.propertyValues(ABP, QueryResultEdge(abEdge, db.versions(abEdge), true))
        prop.values.size should be (1)
        prop.values.head should be ("data1")
      }
    }

    scenario("update edge") {
      withDb(simpleEdgeSchema) { db =>
        import simpleEdgeSchema._
        val ABP = AB.AB_prop_0

        val db1 = db.inVersion(1)
        val aNode = db1.createOrUpdateNode(A, NodeData(Seq("aid"), Seq())).value
        val bNode = db1.createOrUpdateNode(B, NodeData(Seq("bid"), Seq())).value
        val abEdge1 = db1.createOrUpdateEdge(AB, EdgeData(Seq("aid"), Seq("bid"), Seq("data1"))).value
        val db2 = db1.inVersion(2)
        val abEdge2 = db2.createOrUpdateEdge(AB, EdgeData(Seq("aid"), Seq("bid"), Seq("data2"))).value
        abEdge1 should be (abEdge2)
        val props = db2.propertyValues(ABP, QueryResultEdge(abEdge1, db.versions(abEdge1), true)).extract
        assert(props.contains((VersionRangeSet(1), "data1")))
        assert(props.contains((VersionRangeSet(2, Version.Head), "data2")))
        assert(props.size === 2)
        val edge = aNode.dbNode.getSingleRelationship(DynamicRelationshipType.withName(AB.name), Direction.OUTGOING)
        val hyperNode = edge.getOtherNode(aNode.dbNode)
        db2.versions(NodeIdentifier(hyperNode)) should be (VersionRangeSet(VersionRange(1, 1), VersionRange(2, Version.Head)))

      }
    }

    scenario("delete node") {
      withDb(singleIdRefSchema) { db =>
        import singleIdRefSchema._

        val db1 = db.inVersion(1)
        val bNode = db1.createOrUpdateNode(B, NodeData(Seq("bid"), Seq())).value
        val aNode = db1.createOrUpdateNode(A, NodeData(Seq("aid", "bid"), Seq())).value
        val db2 = db1.inVersion(2)
        db2.deleteById(aNode)
        db2.validAtHeadVersion(aNode) should be (false)
        db2.versions(aNode) should be (VersionRangeSet(1))

      }
    }

    scenario("Revive Node") {
      val Adata = NodeData(Seq("aid", "bid"), Seq())
      import singleIdRefSchema._
      withDb(singleIdRefSchema) { db =>
        val db0 = db.inVersion(0)
        db0.createOrUpdateNode(B, NodeData(Seq("bid"), Seq()))
        db0.createOrUpdateNode(A, Adata)
        val db1 = db0.inVersion(1)
        db1.deleteById(db.nodes(A).single)
        val db2 = db1.inVersion(2)
        db2.createOrUpdateNode(A, Adata)
        db2.versions(db.edges(A_id_ref0).single) should be (VersionRangeSet(VersionRange(0, 0), VersionRange(2, Version.Head)))

      }
    }
  }

  feature("Invalid operations are disallowed") {
    scenario("Insert 'id-ref'ed node w/o id-ref data") {
      withDb(singleIdRefSchema) { db =>
        val A = db.schema.node("A").value
        intercept[Throwable]{
          val db1 = db.inVersion(1)
          db1.createOrUpdateNode(A, NodeData(Seq("aid"), Seq()))
        }
      }
    }
  }
}
