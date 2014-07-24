package ch.fhnw.imvs.babelfish.example

import ch.fhnw.imvs.babelfish.dsl.core.{ TrailsPrimitives, BfExtensions }
import ch.fhnw.imvs.babelfish.dsl.table.TableTrails

object CarolDSL extends TrailsPrimitives with TableTrails with BfExtensions with CarolDSL

trait CarolDSL { self: TrailsPrimitives =>
  // define high-level DSL extensions here
}
