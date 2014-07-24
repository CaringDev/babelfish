package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import org.neo4j.graphdb.RelationshipType
import scala.collection.JavaConverters._

/** NeoUtil provides low level database functionality. */
private[lowlevel] object NeoUtil {

  def followSingleEdge(node: NeoNode, relationshipType: RelationshipType, direction: NeoDirection): NeoNode =
    node.getSingleRelationship(relationshipType, direction).getOtherNode(node)

  def edges(node: NeoNode, relationshipType: RelationshipType, direction: NeoDirection): Seq[NeoEdge] =
    node.getRelationships(relationshipType, direction).asScala.toSeq

  def followEdges(node: NeoNode, relationshipType: RelationshipType, direction: NeoDirection): Seq[NeoNode] =
    edges(node, relationshipType, direction).map(_.getOtherNode(node))

  def allEdges(node: NeoNode): Seq[NeoEdge] =
    node.getRelationships.asScala.toSeq
}