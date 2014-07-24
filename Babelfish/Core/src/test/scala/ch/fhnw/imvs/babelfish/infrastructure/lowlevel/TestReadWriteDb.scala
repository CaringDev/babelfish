package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.PlainSchemaConfiguration
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.{ IndexReader, IndexWriter }
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version
import ch.fhnw.imvs.babelfish.infrastructure.{ QueryAPI, ReadWriteDb }
import org.neo4j.tooling.GlobalGraphOperations
import scala.collection.JavaConverters._

trait TestReadWriteDb extends QueryAPI with ReadWriteDb {
  self: DbAccessor with IndexWriter with IndexReader with DbReader =>

  def inNextVersion(): TestReadWriteDb =
    inVersion(version.next)

  def inVersion(version: Version): TestReadWriteDb = {
    require(version.v >= db.version.v, "Db version can only be increased")
    dbInVersion(this, version)
  }

  def totalNeoNodeCount: Int =
    GlobalGraphOperations.at(lowlevelDb.neoDb).getAllNodes.asScala.size

  def totalNeoEdgeCount: Int =
    GlobalGraphOperations.at(lowlevelDb.neoDb).getAllRelationships.asScala.size

  private def dbInVersion(trwdb: TestReadWriteDb, version: Version): TestReadWriteDb =
    TestReadWriteDb(new Db(trwdb.lowlevelDb.neoDb, version, new PlainSchemaConfiguration(db.schema)))

  private def lowlevelDb = db
}

object TestReadWriteDb {
  def apply(database: Db): TestReadWriteDb = {
    new TestReadWriteDb with DbAccessor with IndexWriter with IndexReader with DbReader {
      val db = database
    }
  }
}
