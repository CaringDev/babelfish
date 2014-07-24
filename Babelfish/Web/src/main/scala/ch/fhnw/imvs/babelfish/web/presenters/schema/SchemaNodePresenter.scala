package ch.fhnw.imvs.babelfish.web.presenters.schema

import ch.fhnw.imvs.babelfish.schema.SchemaNode

case class SchemaNodePresenter(
  name: String,
  description: String,
  properties: Iterable[SchemaPropertyPresenter])

object SchemaNodePresenter {
  def apply(node: SchemaNode): SchemaNodePresenter =
    SchemaNodePresenter(node.name, node.desc, node.properties.map { SchemaPropertyPresenter(_) })
}