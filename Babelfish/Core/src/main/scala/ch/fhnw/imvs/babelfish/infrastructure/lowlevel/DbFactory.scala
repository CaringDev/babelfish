package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.infrastructure._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.{ IndexWriter, IndexReader }

trait DbAccessor { protected def db: Db }

object DbFactory {

  def createReadWriteDb(database: Db): ReadWriteDb =
    new ReadWriteDb with DbAccessor with IndexReader with IndexWriter {
      val db = database
    }

  def createReadOnlyDb(database: Db): ReadOnlyDb =
    new ReadOnlyDb with DbAccessor with IndexReader {
      val db = database
    }

  def createQueryDb(database: Db): QueryDb =
    new QueryDb with QueryAPI with DbReader with IndexReader with DbAccessor {
      val db = database
    }

  def createMigrationDb(database: Db): MigrationDb =
    new MigrationDb with DbAccessor with IndexReader with IndexWriter {
      val db = database
    }
}

