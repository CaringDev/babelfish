package ch.fhnw.imvs.babelfish.infrastructure

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.IndexReader
import ch.fhnw.imvs.babelfish.infrastructure.versioning.{ VersionRangeSet, Versioned }
import ch.fhnw.imvs.babelfish.schema._
import scala.reflect.ClassTag

trait QueryAPI { this: DbAccessor with IndexReader with DbReader =>

  /** Finds all nodes of a specific type which are valid in the specified versionRange.
    * @param schemaNode the schema type for which nodes are searched
    * @param version the version range the nodes must be valid in (their range must overlap this range)
    * @return the resulting nodes
    */
  def nodes(schemaNode: SchemaNode, version: VersionRangeSet): Iterable[QueryResultNode] =
    nodes(schemaNode).flatMap { queryResultNodeWithRangeFilter(_, version) }

  /** Finds all edges of a specific types which are valid in the specified versionRange.
    * @param schemaEdge the schema type for which edges are searched
    * @param forward if true, return results in 'natural' direction, reversed otherwise
    * @param version the version range the edges must be valid in (their range must overlap with this range)
    * @return the resulting edges
    */
  def edges(schemaEdge: SchemaEdge, forward: Boolean, version: VersionRangeSet): Iterable[QueryResultEdge] =
    edges(schemaEdge).flatMap { queryResultEdgeWithRangeFilter(_, forward, version) }

  /** Returns the target node of the given edge.
    * (A -e-> B): targetNode(e) returns B
    * @param qre the edge to get the node from
    * @return the target node
    */
  def targetNode(qre: QueryResultEdge): QueryResultNode =
    queryResultNodeFromExactRanges(targetNode(qre.id), qre.validRanges)

  /** Returns the source node of the given edge.
    * (A -e-> B): sourceNode(e) returns A
    * @param qre the edge to get the node from
    * @return the source node
    */
  def sourceNode(qre: QueryResultEdge): QueryResultNode =
    queryResultNodeFromExactRanges (sourceNode(qre.id), qre.validRanges)

  /** gets the values of a specific property
    * @param p the property to get
    * @tparam T is the type of the Property Value
    * @return the property values
    */
  def propertyValues[T: ClassTag](p: SchemaElement#SchemaProperty[T], dbe: QueryResult[Identifier]): Versioned[T] =
    versionedProperties(p, dbe.id).limit(dbe.validRanges)

  /** Finds all edges of a type leaving a node.
    * @param schemaEdge the type of the edges to find
    * @param qrn the node from which they should leave
    * @return all found edges
    */
  def validOutgoingEdges(schemaEdge: SchemaEdge, qrn: QueryResultNode): Seq[QueryResultEdge] =
    edgesOutgoing(qrn.id, schemaEdge).flatMap {
      queryResultEdgeWithRangeFilter(_, true, qrn.validRanges)
    }.toSeq

  /** Finds all edges of a type entering a node.
    * @param schemaEdge the type of the edges to find
    * @param qrn the node which they should enter
    * @return all found edges
    */
  def validIncomingEdges(schemaEdge: SchemaEdge, qrn: QueryResultNode): Seq[QueryResultEdge] =
    edgesIncoming(qrn.id, schemaEdge).flatMap {
      queryResultEdgeWithRangeFilter(_, false, qrn.validRanges)
    }.toSeq

  /** Returns a QueryResultEdge from the given EdgeIdentifier using the given validRanges.
    * @param ei the EdgeIdentifier
    * @param validRanges the exact valid ranges to be used with the given EdgeIdentifier
    * @return the QRE
    */
  def queryResultEdgeFromExactRanges(ei: EdgeIdentifier, forward: Boolean, validRanges: VersionRangeSet): QueryResultEdge = {
    require(!validRanges.isEmpty, "validRanges must not be empty")
    QueryResultEdge(ei, validRanges, forward)
  }

  /** Returns a QueryResultEdge over the full valid time of the given identifier intersected with the given rangeFilter.
    * @param ei the edge identifier
    * @param rangeFilter the filter to intersect with the full valid time of the edge identified by ei
    * @return Some[QRE] or None if the intersection is empty
    */
  def queryResultEdgeWithRangeFilter(ei: EdgeIdentifier, forward: Boolean, rangeFilter: VersionRangeSet): Option[QueryResultEdge] = {
    val exactRanges = versions(ei).intersect(rangeFilter)
    if (exactRanges.isEmpty) None
    else Some(queryResultEdgeFromExactRanges(ei, forward, exactRanges))
  }

  /** Returns a QueryResultNode from the given NodeIdentifier using the given validRanges.
    * @param ni the NodeIdentifier
    * @param validRanges the exact valid ranges to be used with the given NodeIdentifier
    * @return the QRN
    */
  def queryResultNodeFromExactRanges(ni: NodeIdentifier, validRanges: VersionRangeSet): QueryResultNode = {
    require(!validRanges.isEmpty, "validRanges must not be empty")
    QueryResultNode(ni, validRanges)
  }

  /** Returns a QueryResultNode over the full valid time of the given identifier intersected with the given rangeFilter.
    * @param ni the node identifier
    * @param rangeFilter the filter to intersect with the full valid time of the node identified by ni
    * @return Some[QRN] or None if the intersection is empty
    */
  def queryResultNodeWithRangeFilter(ni: NodeIdentifier, rangeFilter: VersionRangeSet): Option[QueryResultNode] = {
    val validRanges = versions(ni).intersect(rangeFilter)
    if (validRanges.isEmpty) None
    else Some(queryResultNodeFromExactRanges(ni, validRanges))
  }
}
