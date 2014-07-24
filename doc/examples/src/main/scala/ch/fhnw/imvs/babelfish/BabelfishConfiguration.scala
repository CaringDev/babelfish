package ch.fhnw.imvs.babelfish

import ch.fhnw.imvs.babelfish.example.CarolSchema
import ch.fhnw.imvs.babelfish.example.CarolSchema.PetKind
import ch.fhnw.imvs.babelfish.schema.Schema

object BabelfishConfiguration extends ConfigurationBase {
  // used to allow non-fully-qualified usage of enum types
  // in DSL statements (e.g. PetKind.Dog)
  private val neoEnums = List(PetKind)
  private val neoEnumNames =
    neoEnums.map(_.getClass.getName.replaceAllLiterally("$", ".").init)

  val schema: Schema = CarolSchema.carolsWorld
  val dslBase = "ch.fhnw.imvs.babelfish.example.CarolDSL"
  val dslImports =
    "ch.fhnw.imvs.babelfish.example.CarolSchema.carolsWorld._" ::
      neoEnumNames
}