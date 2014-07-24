package ch.fhnw.imvs.babelfish.web.presenters.versions

import ch.fhnw.imvs.babelfish.infrastructure.versioning.{ VersionRange, Version }

case class VersionRangePresenter(from: Version, to: Version)

object VersionRangePresenter {
  def apply(v: VersionRange): VersionRangePresenter = {
    VersionRangePresenter(v.from, v.to)
  }
}

