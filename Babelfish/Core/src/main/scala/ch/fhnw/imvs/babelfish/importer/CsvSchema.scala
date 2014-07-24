package ch.fhnw.imvs.babelfish.importer

import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaElement, SchemaNode, Schema }
import ch.fhnw.imvs.babelfish.util.{ Logging, RoseTree, Inner, Leaf }
import org.apache.commons.csv.CSVUtils

/** Calculates various CSV related information. */
object CsvSchema extends Logging {

  /** Creates the CSV representation for a given [[ch.fhnw.imvs.babelfish.schema.Schema]].
    *
    * @param schema the schema to create the CSV for
    * @return the CSV string
    */
  def schemaAsCsv(schema: Schema): String = {
    val explained = explainSchema(schema)
    def formatFile(fn: String, props: Seq[NamedSchemaProperties]) = s"$fn.csv:" + CSVUtils.printLine(props.flatten.map(_._1).toArray)
    explained.filterNot {
      case (e: SchemaEdge, _) => e.isId
      case _                  => false
    }.map { case (_, (n, props)) => formatFile(n, props) }.mkString("\n")
  }

  /** Creates a map of [[ch.fhnw.imvs.babelfish.schema.SchemaElement]]s to their name and properties.
    *
    * @param schema the schema to create the mappings for
    * @return the created map of [SchemaElement -> (its name, [its (flattened) properties])]
    */
  def explainSchema(schema: Schema): Map[SchemaElement, (String, Seq[NamedSchemaProperties])] = {
    val nodes = schema.nodes.map { node =>
      (node, (node.name, Seq(idProperties(node), dataProperties(node))))
    }.toMap

    val edges = schema.edges.map { e =>
      (e, (e.name, Seq(idPropertiesQualified(e.from), idPropertiesQualified(e.to), dataProperties(e))))
    }.toMap

    nodes ++ edges
  }

  /** Creates a list of full qualified id properties of a node.
    *
    * @param n the node to create the id list for
    * @return a list of (FQN -> property) tuples where FQN follows the structure Prop>Node#Prop>OtherN#Prop>...
    */
  private def flattenedIds(n: SchemaNode): NamedSchemaProperties = {
    def rec(t: RoseTree[SchemaEdge, SchemaElement#SchemaProperty[Any]], prefix: String): NamedSchemaProperties = {
      t match {
        case Leaf(schemaProperty)   => Seq(prefix + schemaProperty.name -> schemaProperty)
        case Inner(schemaEdge, ids) => ids.flatMap(rec(_, prefix + schemaEdge.name + ">" + schemaEdge.to.name + "#"))
      }
    }
    n.idProperties.flatMap(rec(_, ""))
  }

  private def dataProperties(se: SchemaElement): NamedSchemaProperties = se.dataProperties.map(p => p.name -> p)

  private def idProperties(node: SchemaNode): NamedSchemaProperties = flattenedIds(node)

  private def idPropertiesQualified(node: SchemaNode): NamedSchemaProperties =
    idProperties(node).map(id => node.name + '#' + id._1 -> id._2)
}
