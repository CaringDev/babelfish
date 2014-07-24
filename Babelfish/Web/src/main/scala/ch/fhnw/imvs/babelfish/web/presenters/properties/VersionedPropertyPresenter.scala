package ch.fhnw.imvs.babelfish.web.presenters.properties

import ch.fhnw.imvs.babelfish.web.presenters.versions.VersionedValuePresenter

case class VersionedPropertyPresenter(key: String, values: Iterable[VersionedValuePresenter])

