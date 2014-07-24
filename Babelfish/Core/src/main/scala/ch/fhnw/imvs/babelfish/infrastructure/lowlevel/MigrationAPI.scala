package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.SchemaMappingDefinitions._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.{ IndexReader, IndexWriter }
import ch.fhnw.imvs.babelfish.infrastructure.{ MigrationDb, QueryDb }
import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaNode, SchemaElement }
import org.neo4j.graphdb.{ Transaction, Direction }

trait MigrationAPI { this: DbAccessor with IndexReader with IndexWriter =>

  def queryDb: QueryDb
  def migrationDb: MigrationDb
  def beginTx(): Transaction = db.neoDb.beginTx()

  def addIdProperty(p: SchemaElement#SchemaProperty[_], default: (SchemaElement#SchemaProperty[_] => Any)) {
    logger.debug(s"Adding id property $p")

    val schemaNode = p.parent.asInstanceOf[SchemaNode]
    val nodes = queryDb.nodes(schemaNode)
    if (nodes.nonEmpty) {
      val neoValue: Any = default(p)
      val dbValue = p.neoType.toDbRep(neoValue)
      nodes.foreach { n =>
        n.dbNode.setProperty(p.dbName, dbValue)
        indexWriter.addSinglePropertyValueToIdIndex(db, schemaNode, n.dbNode, p, neoValue)
      }
    }
  }

  def addProperty(p: SchemaElement#SchemaProperty[_], default: (SchemaElement#SchemaProperty[_] => Any)) {
    logger.debug(s"Adding property $p")

    val elements = p.parent match {
      case node: SchemaNode => queryDb.nodes(node)
      case edge: SchemaEdge => queryDb.edges(edge)
    }
    if (elements.nonEmpty) {
      val value = p.neoType.toDbRep(default(p))
      elements.foreach { e =>
        NeoUtil.edges(e.dbNode, PROPERTY_EDGE_TYPE, Direction.OUTGOING).foreach(_.setProperty(p.dbName, value))
      }
    }
  }

  def addNode(n: SchemaNode) {
    logger.debug(s"Adding node $n")
  }

  def addEdge(e: SchemaEdge) {
    logger.debug(s"Adding edge $e")
  }
}

object MigrationAPI {
  def apply(database: Db): MigrationAPI = new MigrationAPI with DbAccessor with IndexWriter with IndexReader {
    protected def db: Db = database
    val queryDb = DbFactory.createQueryDb(db)
    val migrationDb = DbFactory.createMigrationDb(db)
  }
}
