package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.PlainSchemaConfiguration
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.{ IndexWriter, IndexReader }
import ch.fhnw.imvs.babelfish.infrastructure.{ MigrationDb, QueryAPI, QueryDb, Migrator }
import ch.fhnw.imvs.babelfish.schema._
import org.scalatest.{ Matchers, FeatureSpec }
import scala.collection.mutable

/*
~test-only ch.fhnw.imvs.babelfish.infrastructure.lowlevel.MigratorTest
*/
class MigratorTest extends FeatureSpec with Matchers {

  class TestMigrator(s: Schema) extends MigrationAPI with DbAccessor with IndexWriter with IndexReader {
    protected def db: Db = ???
    def migrationDb = new MigrationDb with DbAccessor with IndexReader with IndexWriter {
      protected lazy val db: Nothing = ???
      override def removeProperties(p: SchemaElement#SchemaProperty[_]) { removedProperties += p }
    }
    val queryDb = new QueryDb with QueryAPI with DbReader with IndexReader with DbAccessor {
      protected lazy val db: Nothing = ???
      override val schema: Schema = s
    }
    val addedProperties = mutable.MutableList[SchemaElement#SchemaProperty[_]]()
    val addedIdProperties = mutable.MutableList[SchemaElement#SchemaProperty[_]]()
    val removedProperties = mutable.MutableList[SchemaElement#SchemaProperty[_]]()
    val addedNodes = mutable.MutableList[SchemaNode]()
    val addedEdges = mutable.MutableList[SchemaEdge]()
    override def addProperty(p: SchemaElement#SchemaProperty[_], default: (SchemaElement#SchemaProperty[_] => Any)) { addedProperties += p }
    override def addIdProperty(p: SchemaElement#SchemaProperty[_], default: (SchemaElement#SchemaProperty[_] => Any)) { addedIdProperties += p }
    override def addNode(n: SchemaNode) { addedNodes += n }
    override def addEdge(e: SchemaEdge) { addedEdges += e }
  }

  object simpleSchema extends Schema("Single") {
    object Node extends SchemaNode {
      object A extends SchemaProperty[Int]
      def properties: Seq[SchemaProperty[_]] = Seq(A)
      def desc: String = ""
      def id: Seq[Identity] = Seq(A)
    }
    def nodes = Seq(Node); def edges = Seq()
  }

  object singleNonIdProp extends Schema("Single") {
    object Node extends SchemaNode {
      object A extends SchemaProperty[Int]
      object B extends SchemaProperty[String]
      def properties: Seq[SchemaProperty[_]] = Seq(A, B)
      def desc: String = ""
      def id: Seq[Identity] = Seq(A)
    }
    def nodes = Seq(Node); def edges = Seq()
  }

  feature("Id properties") {
    scenario("Rename id prop") {
      object s2 extends Schema("Single") {
        object Node extends SchemaNode {
          object A2 extends SchemaProperty[Int]
          def properties: Seq[SchemaProperty[_]] = Seq(A2)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A2)
        }
        def nodes = Seq(Node); def edges = Seq()
      }
      val e = intercept[IllegalArgumentException] {
        Migrator.autoMigrate(new TestMigrator(simpleSchema), new PlainSchemaConfiguration(s2))
      }
      e.getMessage should startWith ("Removing id properties")
    }

    scenario("Adding id prop") {
      object s2 extends Schema("Single") {
        object Node extends SchemaNode {
          object A extends SchemaProperty[Int]
          object A2 extends SchemaProperty[String]
          def properties: Seq[SchemaProperty[_]] = Seq(A, A2)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A, A2)
        }
        def nodes = Seq(Node); def edges = Seq()
      }
      val mAPI = new TestMigrator(simpleSchema)
      Migrator.autoMigrate(mAPI, new PlainSchemaConfiguration(s2, ""))
      mAPI.addedIdProperties should be (Seq(s2.Node.A2))
      mAPI.addedProperties should be ('empty)
    }

    scenario("Changing id prop") {
      object s2 extends Schema("Single") {
        object Node extends SchemaNode {
          object A extends SchemaProperty[String]
          def properties: Seq[SchemaProperty[_]] = Seq(A)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A)
        }
        def nodes = Seq(Node); def edges = Seq()
      }
      val e = intercept[IllegalArgumentException] {
        Migrator.autoMigrate(new TestMigrator(simpleSchema), new PlainSchemaConfiguration(s2))
      }
      e.getMessage should startWith ("Removing id properties")
    }
  }

  feature("Properties") {
    scenario("Removing property") {
      object s2 extends Schema("Single") {
        object Node extends SchemaNode {
          object A extends SchemaProperty[Int]
          def properties: Seq[SchemaProperty[_]] = Seq(A)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A)
        }
        def nodes = Seq(Node); def edges = Seq()
      }
      val m = new TestMigrator(singleNonIdProp)
      Migrator.autoMigrate(m, new PlainSchemaConfiguration(s2))
      assert(m.removedProperties.contains(singleNonIdProp.Node.B))
    }

    scenario("Moving property") {
      object s2 extends Schema("Single") {
        object Node extends SchemaNode {
          object A extends SchemaProperty[Int]
          def properties: Seq[SchemaProperty[_]] = Seq(A)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A)
        }
        object NodeB extends SchemaNode {
          object A extends SchemaProperty[Int]
          object B extends SchemaProperty[String]
          def properties: Seq[SchemaProperty[_]] = Seq(A, B)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A)
        }

        def nodes = Seq(Node, NodeB); def edges = Seq()
      }
      val m = new TestMigrator(singleNonIdProp)
      Migrator.autoMigrate(m, new PlainSchemaConfiguration(s2))
      assert(m.removedProperties.contains(singleNonIdProp.Node.B))
    }

    scenario("Adding property") {
      object s2 extends Schema("Single") {
        object Node extends SchemaNode {
          object A extends SchemaProperty[Int]
          object B extends SchemaProperty[String]
          object C extends SchemaProperty[Long]
          def properties: Seq[SchemaProperty[_]] = Seq(A, B, C)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A)
        }
        def nodes = Seq(Node); def edges = Seq()
      }

      val mAPI = new TestMigrator(singleNonIdProp)
      Migrator.autoMigrate(mAPI, new PlainSchemaConfiguration(s2, null))
      mAPI.addedProperties should have size 1
      assert(mAPI.addedProperties.contains(s2.Node.C))
    }
  }

  feature("Nodes") {
    scenario("Add node") {
      object node2 extends Schema("Single") {
        object Node extends SchemaNode {
          object A extends SchemaProperty[Int]
          def properties: Seq[SchemaProperty[_]] = Seq(A)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A)
        }
        object NodeB extends SchemaNode {
          object A extends SchemaProperty[Int]
          def properties: Seq[SchemaProperty[_]] = Seq(A)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A)
        }
        def nodes = Seq(Node, NodeB); def edges = Seq()
      }
      val mAPI = new TestMigrator(simpleSchema)
      Migrator.autoMigrate(mAPI, new PlainSchemaConfiguration(node2))
      mAPI.addedProperties should be (Nil)
      mAPI.addedEdges should be (Nil)
      mAPI.addedNodes should have size 1
      assert(mAPI.addedNodes.contains(node2.NodeB))
    }
  }

  feature("Edges") {
    scenario("Add edge") {
      object s2 extends Schema("Single") {
        object Node extends SchemaNode {
          object A extends SchemaProperty[Int]
          def properties: Seq[SchemaProperty[_]] = Seq(A)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A)
        }
        object Edge extends SchemaEdge {
          type From = Node.type; type To = Node.type
          def from: Edge.From = Node; def to: Edge.To = Node
          def properties: Seq[SchemaProperty[_]] = Seq()
          def desc: String = ""
          override def toCard = Cardinality.?
        }
        def nodes = Seq(Node); def edges = Seq(Edge)
      }
      val mAPI = new TestMigrator(simpleSchema)
      Migrator.autoMigrate(mAPI, new PlainSchemaConfiguration(s2))
      mAPI.addedEdges should have size 1
      assert(mAPI.addedEdges.contains(s2.Edge))
      mAPI.addedNodes should be (Nil)
      mAPI.addedProperties should be (Nil)
    }

    scenario("Add edge with 'at least one' card") {
      object s2 extends Schema("Single") {
        object Node extends SchemaNode {
          object A extends SchemaProperty[Int]
          def properties: Seq[SchemaProperty[_]] = Seq(A)
          def desc: String = ""
          def id: Seq[Identity] = Seq(A)
        }
        object Edge extends SchemaEdge {
          type From = Node.type; type To = Node.type
          def from: Edge.From = Node; def to: Edge.To = Node
          def properties: Seq[SchemaProperty[_]] = Seq()
          def desc: String = ""
        }
        def nodes = Seq(Node); def edges = Seq(Edge)
      }
      val e = intercept[IllegalArgumentException]{
        val mAPI = new TestMigrator(simpleSchema)
        Migrator.autoMigrate(mAPI, new PlainSchemaConfiguration(s2))
      }
      assert(e.getMessage.startsWith("Adding edges"))
    }
  }

  feature("Node with id edge") {

  }
}
