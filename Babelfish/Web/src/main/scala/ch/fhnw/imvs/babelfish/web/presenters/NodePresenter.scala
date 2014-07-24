package ch.fhnw.imvs.babelfish.web.presenters

import ch.fhnw.imvs.babelfish.infrastructure.{ QueryResultNode, QueryDb }
import ch.fhnw.imvs.babelfish.web.presenters.versions._

case class NodePresenter(
  nodeId: Long,
  nodeType: String,
  idProperties: Map[String, String],
  validRanges: Iterable[VersionRangePresenter],
  properties: Option[Map[String, Iterable[VersionedValuePresenter]]]) extends ElementPresenter

object NodePresenter extends IdentityValueReader {
  def apply(db: QueryDb, qrn: QueryResultNode, includeProperties: Boolean = true): NodePresenter = {

    val schemaNode = db.nodeType(qrn.id)

    val validRanges = qrn.validRanges.versions.map { VersionRangePresenter(_) }
    val properties = if (includeProperties)
      Some(
        schemaNode.properties.filterNot(_.isId).map { p =>
          p.dbName -> VersionedValuePresenters(db.propertyValues(p, qrn))
        }.toMap)
    else
      None

    NodePresenter(qrn.numericId, schemaNode.name, identityValues(db, qrn.id), validRanges, properties)
  }
}