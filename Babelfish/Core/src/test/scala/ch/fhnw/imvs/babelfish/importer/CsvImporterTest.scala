package ch.fhnw.imvs.babelfish.importer

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.DbTest
import ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRangeSet
import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaNode, Schema }
import ch.fhnw.imvs.babelfish.util.FileUtil.getResourceAsFile
import ch.fhnw.imvs.babelfish.util.Logging
import java.util.zip.ZipFile
import org.scalatest.{ Matchers, FeatureSpec }

/*
~test-only ch.fhnw.imvs.babelfish.importer.CsvImporterTest
*/
class CsvImporterTest extends FeatureSpec with Matchers with Logging {

  object emptySchema extends Schema("Empty") {

    def nodes: Seq[SchemaNode] = Seq()
    def edges: Seq[SchemaEdge] = Seq()
  }

  object simpleSchema extends Schema("SimpleSchema") {
    object Node extends SchemaNode {
      def desc = ""

      object ID extends SchemaProperty[Int]
      object Prop extends SchemaProperty[String]

      def id = Seq(ID)
      val properties: Seq[SchemaProperty[_]] = Seq(ID, Prop)
    }

    object Edge extends SchemaEdge {
      def desc = ""
      type From = Node.type; type To = Node.type
      def from = Node; def to = Node

      object Ep extends SchemaProperty[Vector[Double]]
      val properties: Seq[SchemaProperty[_]] = Seq(Ep)
    }

    def nodes: Seq[SchemaNode] = Seq(Node)
    def edges: Seq[SchemaEdge] = Seq(Edge)

  }
  val sn = simpleSchema.Node
  val se = simpleSchema.Edge

  val emptyZipFile = new ZipFile(getResourceAsFile("/Empty.zip"))
  val simpleSchemaZipFile = new ZipFile(getResourceAsFile("/SimpleSchema.zip"))
  val emptyZipFileForSimpleSchemaFullImport = new ZipFile(getResourceAsFile("/EmptyForSimpleSchemaFullImport.zip"))
  val emptyForSimpleMissingIncZip = new ZipFile(getResourceAsFile("/EmptyForSimple-MissingFileIncremental.zip"))
  val emptyForSimpleMissingFullZip = new ZipFile(getResourceAsFile("/EmptyForSimple-MissingFileFull.zip"))

  feature("Zip files can be imported into a database.") {

    scenario("Incremental import with empty data set") {
      DbTest.withDb(emptySchema){ db =>
        val nc = db.totalNeoNodeCount
        CsvImporter.loadIntoDb(db, emptyZipFile, None)
        db.totalNeoNodeCount should be (nc)
      }
    }

    scenario("Incremental import with simple data set"){
      DbTest.withDb(simpleSchema){ db =>
        val nc = db.totalNeoNodeCount
        CsvImporter.loadIntoDb(db, simpleSchemaZipFile, None)

        db.totalNeoNodeCount should be (nc + 3) // 2*DataNode + HyperEdge
      }
    }

    scenario("Full data import with empty data set") {

      val nodesInZip = 2
      val edgesInZip = 1

      DbTest.withDb(simpleSchema){ db =>
        val db1 = db.inVersion(1)
        CsvImporter.loadIntoDb(db, simpleSchemaZipFile, None)
        db.nodes(sn, VersionRangeSet(db.version)) should have size (nodesInZip)
        db.edges(se, true, VersionRangeSet(db.version)) should have size (edgesInZip)
        val db2 = db1.inNextVersion
        CsvImporter.loadIntoDb(db2, emptyZipFileForSimpleSchemaFullImport, None)

        db.nodes(sn, VersionRangeSet(1)) should have size (nodesInZip)
        db.nodes(sn, VersionRangeSet(2)) should be ('empty)
        db.edges(se, true, VersionRangeSet(1)) should have size (edgesInZip)
        db.edges(se, true, VersionRangeSet(2)) should be ('empty)
      }
    }

    scenario("Incremental data import with missing file"){
      DbTest.withDb(simpleSchema){ db =>
        val nodeCountBefore = db.totalNeoNodeCount
        val edgeCountBefore = db.totalNeoEdgeCount
        CsvImporter.loadIntoDb(db, emptyForSimpleMissingIncZip, None)
        // Result should be a successful execution without any change
        db.totalNeoNodeCount should be (nodeCountBefore)
        db.totalNeoEdgeCount should be (edgeCountBefore)
      }
    }

    scenario("Full data import with missing file"){
      DbTest.withDb(simpleSchema){ db =>
        val nodeCountBefore = db.totalNeoNodeCount
        val edgeCountBefore = db.totalNeoEdgeCount

        CsvImporter.loadIntoDb(db, emptyForSimpleMissingFullZip, None)

        // Result should be a successful execution without change
        db.totalNeoNodeCount should be (nodeCountBefore)
        db.totalNeoEdgeCount should be (edgeCountBefore)
      }
    }
  }
}
