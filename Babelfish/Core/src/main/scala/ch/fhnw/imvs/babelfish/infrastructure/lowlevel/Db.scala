package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.InternalConfiguration
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version
import ch.fhnw.imvs.babelfish.schema.Schema

/** Db represents the neo4j database, with additional information for use in Babelfish */
final class Db private[infrastructure] (private[lowlevel] val neoDb: NeoDb, val version: Version, val config: InternalConfiguration) {
  val schema: Schema = config.schema
  private[infrastructure] def shutdown(): Unit =
    neoDb.shutdown()
}