package ch.fhnw.imvs.babelfish.web.presenters

import ch.fhnw.imvs.babelfish.infrastructure.{ QueryResultEdge, QueryDb }
import ch.fhnw.imvs.babelfish.web.presenters.versions.{ VersionedValuePresenters, VersionedValuePresenter, VersionRangePresenter }

case class EdgePresenter(
  edgeId: Long,
  fromNode: Long,
  toNode: Long,
  relationshipType: String,
  validRanges: Iterable[VersionRangePresenter],
  properties: Option[Map[String, Iterable[VersionedValuePresenter]]]) extends ElementPresenter

object EdgePresenter {

  def apply(db: QueryDb, qre: QueryResultEdge, includeProperties: Boolean = true): EdgePresenter = {

    val edgeId = qre.numericId
    val fromNode = db.sourceNode(qre).numericId
    val toNode = db.targetNode(qre).numericId
    val relType = db.edgeType(qre.id).name
    val validRanges = qre.validRanges.versions.map(VersionRangePresenter(_))

    val schemaEdge = db.edgeType(qre.id)
    val properties = if (includeProperties)
      Some(
        schemaEdge.properties.map { p =>
          p.dbName -> VersionedValuePresenters(db.propertyValues(p, qre))
        }.toMap)
    else
      None

    EdgePresenter(edgeId, fromNode, toNode, relType, validRanges, properties)
  }
}