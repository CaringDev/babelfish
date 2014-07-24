package ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel._
import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaNode, SchemaElement }
import ch.fhnw.imvs.babelfish.util.{ Inner, Leaf, RoseTree }
import org.apache.lucene.queryParser.QueryParser
import org.neo4j.graphdb.{ DynamicRelationshipType, Direction }

private[infrastructure] object IndexUtil extends IndexReader with IndexWriter {

  private[lowlevel] def removeFromIdIndex(db: NeoDb, schemaNode: SchemaNode, idNode: NeoNode): Unit =
    indexReader.idIndex(db, schemaNode).remove(idNode)

  private[lowlevel] def removeSinglePropertyFromIdIndex(db: NeoDb, p: SchemaElement#SchemaProperty[_], n: NeoNode): Unit =
    p.parent match {
      case _: SchemaEdge            => throw new IllegalArgumentException(s"Cannot remove edge property $p from id index")
      case sn: SchemaNode if p.isId => indexReader.idIndex(db, sn).remove(n, QueryParser.escape(p.dbName))
      case _: SchemaNode if !p.isId => throw new IllegalArgumentException(s"Cannot remove non-ID property $p from id index")
    }

  private[lowlevel] def renameIdIndex(db: Db, from: SchemaNode, to: SchemaNode) {
    def idProperties(n: NeoNode, idTree: Seq[RoseTree[SchemaEdge, SchemaElement#SchemaProperty[Any]]]): Seq[RoseTree[SchemaEdge, (SchemaElement#SchemaProperty[Any], Any)]] = {
      idTree.map {
        case Leaf(p)            => Leaf((p, p.neoType.fromDbRep(n.getProperty(p.dbName))))
        case Inner(e, children) => Inner(e, idProperties(NeoUtil.followSingleEdge(n, DynamicRelationshipType.withName(e.name), Direction.OUTGOING), children))
      }
    }

    indexReader.findNodesInTypeIndex(db, from).foreach { n =>
      indexWriter.addToIdIndex(db, to, idProperties(n.dbNode, to.idProperties), n.dbNode)
    }
    indexReader.idIndex(db.neoDb, from).delete()
  }
}