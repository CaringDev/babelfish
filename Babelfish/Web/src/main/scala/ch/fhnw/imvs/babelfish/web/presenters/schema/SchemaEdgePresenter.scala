package ch.fhnw.imvs.babelfish.web.presenters.schema

import ch.fhnw.imvs.babelfish.schema.SchemaEdge

case class SchemaEdgePresenter(
  name: String,
  description: String,
  properties: Iterable[SchemaPropertyPresenter],
  fromSchemaNode: String,
  fromCardinality: String,
  toSchemaNode: String,
  toCardinality: String,
  isId: Boolean)

object SchemaEdgePresenter {
  def apply(e: SchemaEdge): SchemaEdgePresenter =
    SchemaEdgePresenter(
      e.name,
      e.desc,
      e.properties.map(SchemaPropertyPresenter(_)),
      e.from.name,
      e.fromCard.toString,
      e.to.name,
      e.toCard.toString,
      e.isId)
}