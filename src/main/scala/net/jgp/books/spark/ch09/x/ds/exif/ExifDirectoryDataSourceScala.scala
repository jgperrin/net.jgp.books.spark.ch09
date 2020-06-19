package net.jgp.books.spark.ch09.x.ds.exif

//import java.util.Map
import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.RelationProvider
import org.slf4j.LoggerFactory
import net.jgp.books.spark.ch09.x.extlib.RecursiveExtensionFilteredLister
import net.jgp.books.spark.ch09.x.utils.K
import scala.collection.immutable.Map

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
  override def createRelation(sqlContext: SQLContext, params: Map[String, String]): BaseRelation = {
    log.debug("-> createRelation()")
    val optionsAsJavaMap = mapAsJavaMapConverter(params).asJava
    // Creates a specifif EXIF relation
    val br = new ExifDirectoryRelation
    br.setSqlContext(sqlContext)
    // Defines the process of acquiring the data through listing files
    val photoLister = new RecursiveExtensionFilteredLister
    import scala.collection.JavaConversions._
    for (entry <- optionsAsJavaMap.entrySet) {
      val key = entry.getKey.toLowerCase
      val value = entry.getValue
      log.debug(s"[$key] --> [$value]")
      key match {
        case K.PATH =>
          photoLister.setPath(value)

        case K.RECURSIVE =>
          if (value.toLowerCase.charAt(0) == 't') photoLister.setRecursive(true)
          else photoLister.setRecursive(false)

        case K.LIMIT =>
          var limit = 0
          try limit = Integer.valueOf(value)
          catch {
            case e: NumberFormatException =>
              log.error(s"Illegal value for limit, expecting a number, got: ${value}. ${e.getMessage}. Ignoring parameter.")
              limit = -1
          }
          photoLister.setLimit(limit)

        case K.EXTENSIONS =>
          val extensions = value.split(",")
          for (i <- 0 until extensions.length) {
            photoLister.addExtension(extensions(i))
          }

        case _ =>
          log.warn("Unrecognized parameter: [{}].", key)

      }
    }
    br.setPhotoLister(photoLister)
    br
  }

}
