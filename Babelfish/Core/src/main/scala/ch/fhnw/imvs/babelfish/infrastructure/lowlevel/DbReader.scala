package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.InternalConfiguration
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.SchemaMappingDefinitions._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.IndexReader
import ch.fhnw.imvs.babelfish.infrastructure.versioning.{ Version, Versioned, VersionRangeSet, VersionRange }
import ch.fhnw.imvs.babelfish.schema._
import ch.fhnw.imvs.babelfish.util.{ Individualize, Logging }
import org.neo4j.graphdb.{ Transaction, DynamicRelationshipType, Direction }
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try

trait DbReader extends Logging { this: DbAccessor with IndexReader =>

  def version: Version = db.version

  def schema: Schema = db.schema

  def config: InternalConfiguration = db.config

  /** Every access to the database must be within a transaction
    * @return a new transaction
    */
  def beginTx(): Transaction = db.neoDb.beginTx()

  def nodes(schemaNode: SchemaNode): Iterable[NodeIdentifier] =
    indexReader.findNodesInTypeIndex(db, schemaNode)

  def edges(schemaEdge: SchemaEdge): Iterable[EdgeIdentifier] =
    indexReader.findEdgesInTypeIndex(db, schemaEdge)

  def targetNode(ei: EdgeIdentifier): NodeIdentifier =
    NodeIdentifier(NeoUtil.followSingleEdge(ei.dbNode, ei.relType, Direction.OUTGOING))

  def sourceNode(ei: EdgeIdentifier): NodeIdentifier =
    NodeIdentifier(NeoUtil.followSingleEdge(ei.dbNode, ei.relType, Direction.INCOMING))

  def edgesOutgoing(ni: NodeIdentifier, schemaEdge: SchemaEdge): Iterable[EdgeIdentifier] = {
    val relType = DynamicRelationshipType.withName(schemaEdge.name)
    NeoUtil.followEdges(ni.dbNode, relType, Direction.OUTGOING).map(EdgeIdentifier(_)(relType))
  }

  def edgesIncoming(ni: NodeIdentifier, schemaEdge: SchemaEdge): Iterable[EdgeIdentifier] = {
    val relType = DynamicRelationshipType.withName(schemaEdge.name)
    NeoUtil.followEdges(ni.dbNode, relType, Direction.INCOMING).map(EdgeIdentifier(_)(relType))
  }

  /** Returns the values of a given property for an identifier in the versions they are valid in.
    * @param p the property
    * @param id the identifier
    * @tparam T the type of property p
    * @return the versioned property values
    */
  def versionedProperties[T: ClassTag](p: SchemaElement#SchemaProperty[T], id: Identifier): Versioned[T] = {
    if (p.isId) {
      Versioned(versions(id), property(p, id.dbNode))
    } else {
      val values = propertyEdges(id.dbNode).map { e => (versionRange(e), property(p, e)) }
      Versioned(values.toVector)
    }
  }

  def findEdgeById(numericId: Long): Option[EdgeIdentifier] = for {
    edgeNode <- Try(db.neoDb.getNodeById(numericId)).toOption
    label <- edgeNode.getLabels.asScala.singleOption
    name = label.name
    _ <- db.schema.edge(name)
  } yield EdgeIdentifier(edgeNode)(DynamicRelationshipType.withName(name))

  def findNodeById(numericId: Long): Option[NodeIdentifier] = for {
    node <- Try(db.neoDb.getNodeById(numericId)).toOption
    label <- node.getLabels.asScala.singleOption
    _ <- db.schema.node(label.name)
  } yield NodeIdentifier(node)

  def findNodeByFullId(sn: SchemaNode, idValues: Seq[Any]): Option[NodeIdentifier] =
    indexReader.findInFullIdIndex(db, sn, idValues)

  /** Extracts the SchemaNode from a node identifier.
    * @param node the node identifier to get the schema node for
    * @return the schema node
    */
  def nodeType(node: NodeIdentifier): SchemaNode =
    db.schema.node(node.dbNode.getLabels.asScala.single.name()).get

  /** Extracts the SchemaEdge from an edge identifier.
    * @param edge the edge identifier to get the schema edge for
    * @return the schema edge
    */
  def edgeType(edge: EdgeIdentifier): SchemaEdge =
    db.schema.edge(edge.relType.name).get

  def validAtHeadVersion(id: Identifier): Boolean =
    versions(id).versions.exists(_.to == Version.Head)

  /** Returns all versions of the given identifier.
    * @param id the identifier
    * @return all versions of the identifier
    */
  def versions(id: Identifier): VersionRangeSet = {
    val propEdges = propertyEdges(id.dbNode)
    VersionRangeSet(propEdges.map(versionRange).toVector)
  }

  private[lowlevel] def property[T](p: SchemaElement#SchemaProperty[T], neoPropertyContainer: NeoPropertyContainer): T =
    p.neoType.fromDbRep(neoPropertyContainer.getProperty(p.dbName))

  private[infrastructure] def findEdge(from: NodeIdentifier, to: NodeIdentifier, schemaEdge: SchemaEdge): Option[EdgeIdentifier] = {
    val relType = DynamicRelationshipType.withName(schemaEdge.name)
    (for {
      hyperNode <- NeoUtil.followEdges(from.dbNode, relType, Direction.OUTGOING)
      if NeoUtil.followSingleEdge(hyperNode, relType, Direction.OUTGOING) == to.dbNode
    } yield EdgeIdentifier(hyperNode)(relType)).headOption
  }

  private[lowlevel] def versionRange(neoEdge: NeoEdge): VersionRange =
    VersionRange(neoEdge.getProperty(VERSION_RANGE_PROPERTY_KEY).asInstanceOf[Long])

  private[lowlevel] def propertyEdges(idNode: NeoNode): Iterable[NeoEdge] =
    NeoUtil.edges(idNode, PROPERTY_EDGE_TYPE, Direction.OUTGOING)
}
