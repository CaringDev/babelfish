package ch.fhnw.imvs.babelfish.web.presenters.versions

import ch.fhnw.imvs.babelfish.infrastructure.versioning.Versioned

case class VersionedValuePresenter(version: VersionRangePresenter, value: Any)

/** Object to create a Seq[VersionedValuePresenters]. */
object VersionedValuePresenters {
  def apply(propertyValues: Versioned[Any]): Seq[VersionedValuePresenter] = {
    propertyValues.extract.flatMap {
      case (validRanges, value) => {
        val serializedValue = value match {
          case v: Vector[_] => v.mkString("[", ", ", "]")
          case x            => x.toString
        }
        validRanges.versions.map { v => VersionedValuePresenter(VersionRangePresenter(v), serializedValue) }
      }
    }
  }
}
