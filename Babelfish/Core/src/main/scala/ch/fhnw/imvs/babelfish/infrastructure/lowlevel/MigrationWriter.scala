package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.importer.{ EdgeData, NodeData }
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.SchemaMappingDefinitions._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.{ IndexUtil, IndexReader }
import ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRange
import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaNode, SchemaElement }
import org.neo4j.graphdb.{ DynamicLabel, DynamicRelationshipType, Direction }

/** Provides lowlevel (i.e. destructive) means of altering the database */
trait MigrationWriter { this: DbAccessor with DbReader with DbWriter with IndexReader =>

  def idProperty[T](p: SchemaElement#SchemaProperty[T], id: NodeIdentifier): T = {
    assert(p.isId, s"$p is not an ID property")
    property(p, id.dbNode)
  }

  def addOrUpdateNode(sn: SchemaNode, data: NodeData, version: VersionRange): NodeIdentifier =
    createOrUpdateNode(sn, data, version).get

  def addOrUpdateEdge(se: SchemaEdge, data: EdgeData, version: VersionRange): EdgeIdentifier =
    createOrUpdateEdge(se, data, version).get

  def removeNode(id: NodeIdentifier) {
    NeoUtil.edges(id.dbNode, PROPERTY_EDGE_TYPE, Direction.BOTH).foreach(_.delete())
    IndexUtil.removeFromIdIndex(db.neoDb, nodeType(id), id.dbNode)
    id.dbNode.delete()
  }

  def removeNodes(sn: SchemaNode) {
    nodes(sn).foreach(removeNode)
  }

  def removeEdge(id: EdgeIdentifier) {
    NeoUtil.edges(id.dbNode, id.relType, Direction.BOTH).foreach(_.delete())
    NeoUtil.edges(id.dbNode, PROPERTY_EDGE_TYPE, Direction.BOTH).foreach(_.delete())
    id.dbNode.delete()
  }

  def removeEdges(se: SchemaEdge) {
    edges(se).foreach(removeEdge)
  }

  def removeProperties(p: SchemaElement#SchemaProperty[_]) = (p.parent match {
    case n: SchemaNode => nodes(n)
    case e: SchemaEdge => edges(e)
  }).foreach(removeProperty(_, p))

  private def removeProperty(elem: Identifier, p: SchemaElement#SchemaProperty[_]) {
    if (p.isId) {
      elem.dbNode.removeProperty(p.dbName)
      IndexUtil.removeSinglePropertyFromIdIndex(db.neoDb, p, elem.dbNode)
    } else NeoUtil.edges(elem.dbNode, PROPERTY_EDGE_TYPE, Direction.BOTH).foreach(_.removeProperty(p.dbName))
    // Compaction (normalization of now equal property edges) is not essential, would be a performance / space improvement
  }

  def renameNodes(from: SchemaNode, to: SchemaNode) {
    nodes(from).foreach { node =>
      node.dbNode.removeLabel(DynamicLabel.label(from.name))
      node.dbNode.addLabel(DynamicLabel.label(to.name))
    }
    IndexUtil.renameIdIndex(db, from, to)
  }

  def renameEdges(from: SchemaEdge, to: SchemaEdge) {
    val relType = DynamicRelationshipType.withName(to.name)
    edges(from).foreach { e =>
      e.dbNode.removeLabel(DynamicLabel.label(from.name))
      e.dbNode.addLabel(DynamicLabel.label(to.name))
      NeoUtil.edges(e.dbNode, e.relType, Direction.BOTH).foreach { lle =>
        lle.getStartNode.createRelationshipTo(lle.getEndNode, relType)
        lle.delete()
      }
    }
  }
}
