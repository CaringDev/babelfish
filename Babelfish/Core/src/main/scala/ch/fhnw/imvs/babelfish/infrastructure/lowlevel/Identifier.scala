package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import org.neo4j.graphdb.RelationshipType

/** A marker for a type which uniquely identifies an edge or a node. */
sealed trait Identifier {
  private[lowlevel] def dbNode: NeoNode
}

/** EdgeIdentifier represents a edge independent of its version. */
final case class EdgeIdentifier private[lowlevel] (private[lowlevel] val dbNode: NeoNode)(private[lowlevel] val relType: RelationshipType /* for performance */ ) extends Identifier {
  private[infrastructure] def numericId(forward: Boolean): Long = {
    val id = dbNode.getId
    if (!forward) -id else id
  }
}

/** NodeIdentifier represents a node independent of its version. */
final case class NodeIdentifier private[lowlevel] (private[lowlevel] val dbNode: NeoNode) extends Identifier {
  private[infrastructure] def numericId = dbNode.getId
}
