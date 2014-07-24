package ch.fhnw.imvs.babelfish

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.MigrationWriter
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.{ IndexWriter, IndexReader }
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.{ DbWriter, DbAccessor, DbReader }

package infrastructure {
  trait ReadOnlyDb extends DbReader { this: DbAccessor with IndexReader => }
  trait ReadWriteDb extends DbWriter with ReadOnlyDb { this: DbAccessor with IndexReader with IndexWriter => }
  trait QueryDb extends ReadOnlyDb with QueryAPI { this: DbAccessor with IndexReader => }
  trait MigrationDb extends ReadWriteDb with MigrationWriter { this: DbAccessor with IndexReader with IndexWriter => }
}
