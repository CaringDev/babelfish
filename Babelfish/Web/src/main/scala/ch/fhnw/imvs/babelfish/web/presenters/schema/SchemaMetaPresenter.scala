package ch.fhnw.imvs.babelfish.web.presenters.schema

import ch.fhnw.imvs.babelfish.schema.Schema

case class SchemaMetaPresenter(name: String, description: String, nodeCount: Int, edgeCount: Int)

object SchemaMetaPresenter {
  def apply(schema: Schema) = new SchemaMetaPresenter(schema.name, schema.desc, schema.nodes.size, schema.edges.size)
}