package ch.fhnw.imvs.babelfish.infrastructure

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.{ Identifier, EdgeIdentifier, NodeIdentifier }
import ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRangeSet

/** QueryResult is the parent of all queries from the DbRead object.
  * It consists of an identifier for an element and the set of valid version intervals.
  * This object represent a result of a query and may contain different versions of one object.
  * @tparam Id is the type representing the identifier of this QueryResult
  */
sealed trait QueryResult[+Id <: Identifier] {

  def id: Id

  /** returns the version ranges for which this QueryResult contains valid objects
    * @return all Version ranges as VersionRangeSet
    */
  def validRanges: VersionRangeSet

  def numericId: Long
}

/** QueryResultEdge is a QueryResultElement which contains an edge as its object.
  * This object represent a result of an edge query and may contain different versions of one edge.
  *
  * A QRE is only valid (wrt. version ranges) if both adjacent nodes are valid.
  * Invariant: validRanges ⊆ VersionRangeSet of source node && validRanges ⊆ VersionRangeSet of target node
  */
final case class QueryResultEdge(id: EdgeIdentifier, validRanges: VersionRangeSet, forward: Boolean) extends QueryResult[EdgeIdentifier] {
  def numericId: Long = id.numericId(forward)
}

/** QueryResultNode is a QueryResultElement which contains a node as its object.
  * This object represent a result of a node query and may contain different versions of one node.
  */
final case class QueryResultNode(id: NodeIdentifier, validRanges: VersionRangeSet) extends QueryResult[NodeIdentifier] {
  def numericId: Long = id.numericId
}

