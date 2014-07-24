package ch.fhnw.imvs.babelfish.importer

import ch.fhnw.imvs.babelfish.importer.ImporterConfiguration._
import java.util.Properties
import java.util.zip.ZipFile
import scala.collection.JavaConverters._

/** Contains constants used in import configuration files */
object ImporterConfiguration {
  val configEntryName = "Babelfish.properties"

  /** Key for the import kind */
  final val IMPORT = "import"

  /** Values for the import kind */
  object Import {
    final val FULL = "full"
    final val INCREMENTAL = "incremental"
  }
}

/** Reads the import configuration file from a ZipFile.
  *
  * The entries must be in the following format:
  * [ELEMENTNAME.]KEY = VALUE
  * where ELEMENTNAME is the name of a node respectively an edge (i.e. the filename without extension)
  * KEYs and VALUEs are listed in the [[ch.fhnw.imvs.babelfish.importer.ImporterConfiguration]] object.
  * @param zipFile the file to read the import configuration from
  */
case class ImporterConfiguration(zipFile: ZipFile) {
  private val props = new Properties()
  private val configEntry = zipFile.getEntry(configEntryName)
  if (configEntry == null) throw new IllegalArgumentException(s"Missing $configEntryName in ${zipFile.getName}")
  private val in = zipFile.getInputStream(configEntry)
  props.load(in)
  in.close()
  /** All properties defined in the file. */
  val properties = props.asScala
  /** Global properties, i.e. not scoped to a element name */
  lazy val globals = properties.filter(!_._1.contains('.'))
  private lazy val locals = properties.filter(_._1.contains('.')).toSeq.map {
    case (k, v) =>
      val key = k.splitAt(k.indexOf('.'))
      (key._1, key._2.drop(1), v)
  }
  /** Scoped properties by element name */
  lazy val byFile = locals.groupBy(_._1).mapValues(_.map(c => c._2 -> c._3).toMap)
  /** Scoped properties by topic, i.e. KEY */
  lazy val byTopic = locals.groupBy(_._2).mapValues(_.map(c => c._1 -> c._3).toMap)
}
