package ch.fhnw.imvs.babelfish.importer

/** Represents data for a single node.
  *
  * @param idValues the parsed id values (in order) for [[ch.fhnw.imvs.babelfish.schema.SchemaNode.idProperties]]
  * @param dataValues the parsed values (in order) for [[ch.fhnw.imvs.babelfish.schema.SchemaNode.dataProperties]]
  */
final case class NodeData(idValues: Seq[Any], dataValues: Seq[Any])

/** Represents data fo a single edge.
  *
  * @param fromIds the parsed id values (in order) identifying the start node
  * @param toIds the parsed id values (in order) identifying the end node
  * @param dataValues the parsed values (in order) for [[ch.fhnw.imvs.babelfish.schema.SchemaEdge.dataProperties]]
  */
final case class EdgeData(fromIds: Seq[Any], toIds: Seq[Any], dataValues: Seq[Any]) {
  override def toString =
    s"EdgeData(${fromIds.mkString("(", ", ", ")")} -> ${toIds.mkString("(", ", ", ")")} with ${dataValues.mkString("(", ", ", ")")})"
}