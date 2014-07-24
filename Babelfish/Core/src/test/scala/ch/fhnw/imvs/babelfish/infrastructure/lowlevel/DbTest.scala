package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.PlainSchemaConfiguration
import ch.fhnw.imvs.babelfish.infrastructure.{ Babelfish, DatabaseManager }
import ch.fhnw.imvs.babelfish.schema.{ SchemaNode, SchemaEdge, Schema }
import java.nio.file.{ Path, Paths }
import org.apache.commons.io.FileUtils
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.test.TestGraphDatabaseFactory
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version

object DbTest {

  object EmptySchema extends Schema("Empty") {
    def edges: Seq[SchemaEdge] = Seq()
    def nodes: Seq[SchemaNode] = Seq()
  }

  def withBf[T](schema: Schema = EmptySchema)(block: (Babelfish, Path) => T): T = {
    val tmpExt = s"bf_${System.currentTimeMillis()}"
    val tempPath = Paths.get(FileUtils.getTempDirectoryPath, tmpExt)
    val bf = Babelfish(tempPath, schema)
    try {
      block(bf, tempPath)
    } finally {
      bf.kill()
      FileUtils.deleteDirectory(tempPath.toFile)
    }
  }

  def withDb[T](schema: Schema)(block: TestReadWriteDb => T): T = {
    val config = new PlainSchemaConfiguration(schema)

    val neoDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cache_type, "strong")
      .setConfig(GraphDatabaseSettings.keep_logical_logs, "false")
      .newGraphDatabase()

    try {
      DatabaseManager.tx(neoDb){
        val db = new Db(neoDb, Version(0), config)
        block(TestReadWriteDb(db))
      }
    } finally {
      neoDb.shutdown()
    }
  }
}
