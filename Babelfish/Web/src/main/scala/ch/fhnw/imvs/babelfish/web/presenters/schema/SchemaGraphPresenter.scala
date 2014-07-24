package ch.fhnw.imvs.babelfish.web.presenters.schema

import ch.fhnw.imvs.babelfish.schema.Schema
import ch.fhnw.imvs.babelfish.schema.neo4j.NeoType.EnumNeoType

case class SchemaGraphPresenter(nodes: Iterable[SchemaNodePresenter], edges: Iterable[SchemaEdgePresenter], enums: Iterable[SchemaEnumPresenter])

object SchemaGraphPresenter {
  def apply(schema: Schema): SchemaGraphPresenter = {
    val enums: Seq[Enumeration] = (schema.nodes ++ schema.edges).flatMap(_.properties.map(_.neoType).collect{ case e: EnumNeoType[_] => e.enum: Enumeration }).distinct
    SchemaGraphPresenter(
      schema.nodes.map { SchemaNodePresenter(_) },
      schema.edges.map { SchemaEdgePresenter(_) },
      enums.map{ e => SchemaEnumPresenter(e.getClass.getName.replaceAllLiterally("$", ".").init, e.values.map(_.toString)) })
  }
}