package net.jgp.books.spark.ch09.x.ds.exif

import java.util
import java.util.Map

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * This is the main class of our data source.
 *
 * @author rambabu.posa
 */
class ExifDirectoryDataSourceScala extends RelationProvider {

  private val log = LoggerFactory.getLogger(classOf[ExifDirectoryDataSourceScala])
  /**
   * Creates a base relation using the Spark's SQL context and a map of
   * parameters (our options)
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    log.debug("-> createRelation()")

    val optionsAsJavaMap: util.Map[String, String] = mapAsJavaMapConverter(params).asJava

  }


}
