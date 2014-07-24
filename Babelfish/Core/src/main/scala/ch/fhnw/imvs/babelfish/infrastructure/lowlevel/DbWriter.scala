package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import ch.fhnw.imvs.babelfish.importer.{ NodeData, EdgeData }
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.SchemaMappingDefinitions._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.index.{ IndexWriter, IndexReader }
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version._
import ch.fhnw.imvs.babelfish.infrastructure.versioning.{ Version, VersionRange }
import ch.fhnw.imvs.babelfish.schema.{ SchemaElement, SchemaEdge, SchemaNode }
import ch.fhnw.imvs.babelfish.util.{ Individualize, Leaf, RoseTree, Logging }
import org.neo4j.graphdb.{ DynamicLabel, DynamicRelationshipType, Direction }

trait DbWriter extends Logging { this: DbAccessor with DbReader with IndexReader with IndexWriter =>

  /** Updates the node identified by the given nodeData or creates a new one if none exists.
    * @param nodeData the data for the node to be created / updated
    * @return the newly created or updated node or None if a eventual parent node could not be found
    */
  def createOrUpdateNode(schemaNode: SchemaNode, nodeData: NodeData): Option[NodeIdentifier] =
    createOrUpdateNode(schemaNode, nodeData, VersionRange(version, Head))

  private[lowlevel] def createOrUpdateNode(schemaNode: SchemaNode, nodeData: NodeData, version: VersionRange): Option[NodeIdentifier] = {
    indexReader.findInFullIdIndex(db, schemaNode, nodeData.idValues) match {
      case node @ Some(idNode) =>
        updateNode(schemaNode, idNode, nodeData, version); node
      case None => createNode(schemaNode, nodeData, version)
    }
  }

  /** Marks the node or edge identified by the given id as deleted as of the current db version.
    * @param id the identifier data for the node or edge to be deleted
    */
  def deleteById(id: Identifier) {
    val deleteVersion = version
    findAsOfVersion(id, deleteVersion) match {
      case Some(ne) =>
        terminatePropertyEdge(ne, deleteVersion)
        id match {
          case n: NodeIdentifier => terminateOutgoingIdEdges(id.dbNode, nodeType(n), deleteVersion)
          case e: EdgeIdentifier => // already done
        }
      case None => logger.warn(s"Tried to delete already terminated element $id")
    }
  }

  /** Updates the edge identified by the given edgeData or creates a new one if none exists.
    * @param edgeData the data for the edge to be created / updated
    * @return the newly created or updated edge or None if at least one of the end nodes could not be found
    */
  def createOrUpdateEdge(schemaEdge: SchemaEdge, edgeData: EdgeData): Option[EdgeIdentifier] =
    createOrUpdateEdge(schemaEdge: SchemaEdge, edgeData, VersionRange(version, Head))

  private[lowlevel] def createOrUpdateEdge(schemaEdge: SchemaEdge, edgeData: EdgeData, version: VersionRange): Option[EdgeIdentifier] = {
    require(!schemaEdge.isId, "Id references are auto generated.")
    (indexReader.findInFullIdIndex(db, schemaEdge.from, edgeData.fromIds), indexReader.findInFullIdIndex(db, schemaEdge.to, edgeData.toIds)) match {
      case (Some(fromNode), Some(toNode)) => findEdge(fromNode, toNode, schemaEdge) match {
        case None =>
          val data = schemaEdge.dataProperties.zip(edgeData.dataValues)
          Some(createEdge(schemaEdge, version, fromNode.dbNode, toNode.dbNode, data))
        case edge @ Some(edgeId) => updateEdge(schemaEdge, edgeId, edgeData, version); edge
      }
      case x =>
        val startEnd = x match {
          case (Some(_), None) => "end"
          case (None, Some(_)) => "start"
          case _               => "start and end"
        }
        logger.error(s"Could not find or create $schemaEdge for $edgeData (missing $startEnd node)")
        None
    }
  }

  def findEdgeByData(schemaEdge: SchemaEdge, fromIds: Seq[Any], toIds: Seq[Any]): Option[EdgeIdentifier] = {
    (for {
      fromNode <- indexReader.findInFullIdIndex(db, schemaEdge.from, fromIds)
      toNode <- indexReader.findInFullIdIndex(db, schemaEdge.to, toIds)
      edgeId <- findEdge(fromNode, toNode, schemaEdge)
    } yield {
      Some(edgeId)
    }).getOrElse(None)
  }

  private def terminateOutgoingIdEdges(neoNode: NeoNode, schemaNode: SchemaNode, deleteVersion: Version) {
    idEdges(neoNode, schemaNode).foreach { idEdgeNode =>
      val mostRecent = propertyEdges(idEdgeNode).filter(neoEdge => versionRange(neoEdge).contains(deleteVersion)).single
      terminatePropertyEdge(mostRecent, deleteVersion)
    }
  }

  private def reviveOutgoingIdEdges(neoNode: NeoNode, schemaNode: SchemaNode, reviveVersion: VersionRange) {
    idEdges(neoNode, schemaNode).foreach { idEdgeNode =>
      addNewPropertyVersionEdge(idEdgeNode, Seq(), reviveVersion)
    }
  }

  private def idEdges(neoNode: NeoNode, schemaNode: SchemaNode): Iterable[NeoNode] = {
    val idSchemaEdges = schemaNode.id.collect { case e: SchemaEdge => e }
    idSchemaEdges.map(idSchemaEdge => {
      val relType = DynamicRelationshipType.withName(idSchemaEdge.name)
      NeoUtil.followSingleEdge(neoNode, relType, Direction.OUTGOING)
    })
  }

  private def updateEdge(schemaEdge: SchemaEdge, edge: EdgeIdentifier, edgeData: EdgeData, version: VersionRange): Unit =
    update(schemaEdge, edge, edgeData.dataValues, version, edgeData.fromIds ++ edgeData.toIds)

  private def updateNode(schemaNode: SchemaNode, node: NodeIdentifier, nodeData: NodeData, version: VersionRange): Unit =
    update(schemaNode, node, nodeData.dataValues, version, nodeData.idValues)

  private def update(schemaElement: SchemaElement, id: Identifier, dataValues: Seq[Any], v: VersionRange, idValues: => Seq[Any]): Unit = {
    val data = schemaElement.dataProperties.zip(dataValues)
    findAsOfVersion(id, v.from) match {
      case Some(propertyEdge) =>
        if (!areAllDataPropertyValuesEqual(data, propertyEdge)) {
          if (versionRange(propertyEdge).from == version) {
            logger.warn(s"Found duplicate ${schemaElement.name} with id ${idValues.mkString("[", ", ", "]")} in this import.")
          } else {
            terminatePropertyEdge(propertyEdge, v.from)
            addNewPropertyVersionEdge(propertyEdge.getStartNode, data, v)
          }
        }
      case None =>
        (schemaElement, id) match {
          case (sn: SchemaNode, NodeIdentifier(dbNode)) => reviveOutgoingIdEdges(dbNode, sn, v)
          case (se: SchemaEdge, EdgeIdentifier(_))      =>
          case (a, b)                                   => throw new MatchError(s"Inconsistent SchemaElement $a and Identifier $b.")
        }
        addNewPropertyVersionEdge(id.dbNode, data, v)
    }
  }

  private def createEdge(schemaEdge: SchemaEdge, version: VersionRange, from: NeoNode, to: NeoNode, data: Seq[(SchemaElement#SchemaProperty[_], Any)]): EdgeIdentifier = {
    val edgeNode = db.neoDb.createNode(DynamicLabel.label(schemaEdge.name))

    val relType = DynamicRelationshipType.withName(schemaEdge.name)
    from.createRelationshipTo(edgeNode, relType)
    edgeNode.createRelationshipTo(to, relType)
    addNewPropertyVersionEdge(edgeNode, data, version)

    EdgeIdentifier(edgeNode)(relType)
  }

  private def hasPropertyValue(prop: SchemaElement#SchemaProperty[_], expectedValue: Any, propContainer: NeoPropertyContainer): Boolean = {
    val actualDbRep: Any = propContainer.getProperty(prop.dbName)
    val expectedDbRep = prop.neoType.toDbRep(expectedValue)
    prop.neoType.equalsDbRep(actualDbRep, expectedDbRep)
  }

  private def areAllDataPropertyValuesEqual(data: Seq[(SchemaElement#SchemaProperty[_], Any)], propertyContainer: NeoPropertyContainer): Boolean =
    data.forall { case (prop, value) => hasPropertyValue(prop, value, propertyContainer) }

  private def terminatePropertyEdge(propertyEdge: NeoEdge, deleteVersion: Version) {
    val previousVersion = versionRange(propertyEdge)
    require(previousVersion.to == Head, "Edge already terminated.")
    val lastValidVersion = deleteVersion.previous
    setVersionRange(propertyEdge, VersionRange(previousVersion.from, lastValidVersion))
  }

  private def setVersionRange(propertyEdge: NeoEdge, version: VersionRange) {
    propertyEdge.setProperty(VERSION_RANGE_PROPERTY_KEY, version.fromTo)
  }

  private def findAsOfVersion(id: Identifier, version: Version): Option[NeoEdge] =
    propertyEdges(id.dbNode).find(e => versionRange(e).contains(version))

  private def addNewPropertyVersionEdge(idNode: NeoNode, data: Seq[(SchemaElement#SchemaProperty[_], Any)], version: VersionRange): Unit = {
    val propertiesRelation = idNode.createRelationshipTo(idNode, PROPERTY_EDGE_TYPE)
    setVersionRange(propertiesRelation, version)
    addProps(propertiesRelation, data)
  }

  private def createNode(schemaNode: SchemaNode, nodeData: NodeData, version: VersionRange): Option[NodeIdentifier] = {
    indexReader.findReferencedIdNodes(db.neoDb, schemaNode, nodeData.idValues) match {
      case None =>
        logger.error(s"Could not create ${schemaNode.name}($nodeData) due to missing parent nodes.")
        None
      case Some(idNodes) =>
        val neoNode = db.neoDb.createNode(DynamicLabel.label(schemaNode.name))

        // add new identity edges
        idNodes.foreach {
          case (schemaEdge, idNode) =>
            createEdge(schemaEdge, version, neoNode, idNode, Seq())
        }

        val pathAndValues = RoseTree.zipTreesWithValues(schemaNode.idProperties, nodeData.idValues)
        val allDirectIdValues = pathAndValues.collect { case Leaf(idValue) => idValue }

        /* Add local id properties (as opposed to id references) */
        addProps(neoNode, allDirectIdValues.collect { case (sp: SchemaElement#SchemaProperty[Any], a) => (sp, a) })
        val data = schemaNode.dataProperties.zip(nodeData.dataValues)
        addNewPropertyVersionEdge(neoNode, data, version)

        indexWriter.addToIdIndex(db, schemaNode, pathAndValues, neoNode)
        Some(NodeIdentifier(neoNode))
    }
  }

  private def addProps(propertyContainer: NeoPropertyContainer, props: Seq[(SchemaElement#SchemaProperty[_], Any)]) {
    props.foreach { case (prop, value) => propertyContainer.setProperty(prop.dbName, prop.neoType.toDbRep(value)) }
  }
}