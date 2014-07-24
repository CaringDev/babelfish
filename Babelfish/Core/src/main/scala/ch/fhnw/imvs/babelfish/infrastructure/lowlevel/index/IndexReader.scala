package ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.SchemaMappingDefinitions.ID_INDEX_PREFIX
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel._
import ch.fhnw.imvs.babelfish.schema.SchemaNode
import ch.fhnw.imvs.babelfish.schema.{ SchemaElement, SchemaEdge }
import ch.fhnw.imvs.babelfish.util.{ Logging, Leaf, Inner, RoseTree }
import org.apache.lucene.index.Term
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.{ BooleanClause, TermQuery, BooleanQuery }
import org.neo4j.graphdb.index.Index
import org.neo4j.graphdb.{ DynamicLabel, DynamicRelationshipType }
import org.neo4j.tooling.GlobalGraphOperations
import scala.collection.JavaConverters._

trait IndexReader extends Logging {

  private[infrastructure] val indexReader = new IndexReaderAPI

  private[IndexReader] class IndexReaderAPI {

    private[lowlevel] def findNodesInTypeIndex(db: Db, sn: SchemaNode): Iterable[NodeIdentifier] =
      GlobalGraphOperations.at(db.neoDb).getAllNodesWithLabel(DynamicLabel.label(sn.name)).asScala.map(NodeIdentifier)

    private[lowlevel] def findEdgesInTypeIndex(db: Db, se: SchemaEdge): Iterable[EdgeIdentifier] = {
      val relType = DynamicRelationshipType.withName(se.name)
      GlobalGraphOperations.at(db.neoDb).getAllNodesWithLabel(DynamicLabel.label(se.name)).asScala.map(EdgeIdentifier(_)(relType))
    }

    /** Returns the node identifier for the given id properties.
      * @param db the database
      * @param schemaNode the requested type of the node
      * @param idValues the id values as found on NodeData
      * @return Some node, or None if none can be found
      */
    def findInFullIdIndex(db: Db, schemaNode: SchemaNode, idValues: Seq[Any]): Option[NodeIdentifier] =
      internalFindInFullIdIndex(db.neoDb, schemaNode, RoseTree.zipTreesWithValues(schemaNode.idProperties, idValues)).map(NodeIdentifier)

    private[lowlevel] def findReferencedIdNodes(db: NeoDb, schemaNode: SchemaNode, idValues: Seq[Any]): Option[Seq[(SchemaEdge, NeoNode)]] = {
      val pathAndValues = RoseTree.zipTreesWithValues(schemaNode.idProperties, idValues)
      val onlyIdReferences = pathAndValues.collect {
        case Inner(schemaEdge, children) => internalFindInFullIdIndex(db, schemaEdge.to, children).map((schemaEdge, _))
      }
      val res = onlyIdReferences.flatten
      if (res.size == onlyIdReferences.size) Some(res)
      else None
    }

    private[index] def resolveFullIdIndexKeyValuePairs(db: NeoDb, paths: Seq[RoseTree[SchemaEdge, (SchemaElement#SchemaProperty[Any], Any /* NeoType */ )]]): Option[Seq[(String, String)]] = {
      def dbRepToIndexString(value: Any /* NeoType */ ): String = value match {
        case arr: Array[_] => arr.mkString("##")
        case _             => value.toString
      }

      def rec(path: RoseTree[SchemaEdge, (SchemaElement#SchemaProperty[Any], Any /* NeoType */ )]): Option[(String, String)] = {
        path match {
          case Leaf((property, propertyValue)) =>
            val keyString = QueryParser.escape(property.dbName)
            val valueString = QueryParser.escape(dbRepToIndexString(property.neoType.toDbRep(propertyValue)))
            Some((keyString, valueString))
          case Inner(schemaEdge, children) =>
            val ids = children.flatMap(rec)
            if (ids.size == children.size) {
              executeQuery(db, schemaEdge.to, ids).map(res => (schemaEdge.name, res.getId.toString))
            } else {
              logger.warn(s"Could not find parent ${schemaEdge.to.name}($children) for ${schemaEdge.from.name}")
              None
            }
        }
      }
      val res = paths.flatMap(rec)
      if (res.size == paths.size) Some(res)
      else None
    }

    /** Returns the id index for id nodes of the given schema type.
      * @param db the database
      * @param sn the schema node
      * @return the index
      */
    private[index] def idIndex(db: NeoDb, sn: SchemaNode): Index[NeoNode] =
      db.index().forNodes(ID_INDEX_PREFIX + sn.name)

    private def executeQuery(db: NeoDb, sn: SchemaNode, kvps: Seq[(String, String)]): Option[NeoNode] = {
      val termQueries = kvps.map(kv => new TermQuery(new Term(kv._1, kv._2)))
      val query = termQueries.foldLeft(new BooleanQuery()) { case (bq, tq) => bq.add(new BooleanClause(tq, Occur.MUST)); bq }
      val result = idIndex(db, sn).query(query)

      if (result.hasNext) {
        val neoNode = result.next()
        assert(!result.hasNext, s"Ambiguous id properties: $kvps for $sn")
        Some(neoNode)
      } else None
    }

    private def internalFindInFullIdIndex(db: NeoDb, sn: SchemaNode, pathsAndValues: Seq[RoseTree[SchemaEdge, (SchemaElement#SchemaProperty[Any], Any /* NeoType */ )]]): Option[NeoNode] =
      for {
        kvps <- resolveFullIdIndexKeyValuePairs(db, pathsAndValues)
        neoNode <- executeQuery(db, sn, kvps)
      } yield neoNode
  }
}
