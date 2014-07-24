package ch.fhnw.imvs.babelfish.web.presenters.schema

import ch.fhnw.imvs.babelfish.schema.SchemaElement

case class SchemaPropertyPresenter(
  name: String,
  description: String,
  propertyType: String,
  isId: Boolean)

object SchemaPropertyPresenter {
  def apply(p: SchemaElement#SchemaProperty[_]): SchemaPropertyPresenter =
    SchemaPropertyPresenter(p.name, p.desc, p.neoType.name, p.isId)
}
