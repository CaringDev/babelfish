package ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel._
import ch.fhnw.imvs.babelfish.schema._
import ch.fhnw.imvs.babelfish.util.{ Leaf, RoseTree }

trait IndexWriter { this: IndexReader =>

  private[lowlevel] val indexWriter = new IndexWriterAPI

  private[IndexWriter] class IndexWriterAPI {

    private[lowlevel] def addSinglePropertyValueToIdIndex[T](db: Db, schemaNode: SchemaNode, idNode: NeoNode, property: SchemaElement#SchemaProperty[T], value: Any /* NeoType */ ): Unit =
      addToIdIndex(db, schemaNode, Seq(Leaf((property.asInstanceOf[SchemaElement#SchemaProperty[Any]], value))), idNode)

    private[lowlevel] def addToIdIndex(db: Db, schemaNode: SchemaNode, pathsAndValues: Seq[RoseTree[SchemaEdge, (SchemaElement#SchemaProperty[Any], Any /* NeoType */ )]], idNode: NeoNode) {
      val fullIdIndex = indexReader.idIndex(db.neoDb, schemaNode)
      val keyValuePairs = indexReader.resolveFullIdIndexKeyValuePairs(db.neoDb, pathsAndValues).get
      keyValuePairs.foreach { case (key, value) => fullIdIndex.add(idNode, key, value) }
    }
  }
}