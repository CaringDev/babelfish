package ch.fhnw.imvs.babelfish.web.presenters

import ch.fhnw.imvs.babelfish.infrastructure.QueryDb
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel._
import ch.fhnw.imvs.babelfish.util.{ RoseTree, Leaf, Inner }
import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaElement }

trait IdentityValueReader {

  /** Returns all id property keys and values (including transitive reference ids) for the given node identifier.
    *
    * @param db the database
    * @param node the node for which to search the id properties
    * @return a map from id property key to the corresponding value
    */
  def identityValues(db: QueryDb, node: NodeIdentifier): Map[String, String] =
    db.nodeType(node).idProperties.flatMap(extractProperties(db, _, node)).map {
      case (schemaProperty, a: Array[_])  => schemaProperty.dbName -> a.mkString("[", ", ", "]")
      case (schemaProperty, a: Vector[_]) => schemaProperty.dbName -> a.mkString("[", ", ", "]")
      case (schemaProperty, x)            => schemaProperty.dbName -> x.toString
    }.toMap

  private def extractProperties(db: QueryDb,
                                rt: RoseTree[SchemaEdge, SchemaElement#SchemaProperty[Any]],
                                node: NodeIdentifier): Seq[(SchemaElement#SchemaProperty[Any], Any)] =
    rt match {
      case Leaf(p)     => Seq(p -> db.versionedProperties(p, node).values.head)
      case Inner(e, t) => t.flatMap(extractProperties(db, _, db.targetNode(db.edgesOutgoing(node, e).head)))
    }
}
