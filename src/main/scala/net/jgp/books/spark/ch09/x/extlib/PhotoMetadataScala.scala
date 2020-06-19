package net.jgp.books.spark.ch09.x.extlib

import java.io.Serializable
import java.nio.file.attribute.FileTime
import java.sql.Timestamp
import java.util.Date
import org.slf4j.LoggerFactory
import net.jgp.books.spark.ch09.x.utils.SparkColumn



/**
 * A good old JavaBean containing the EXIF properties as well as the
 * SparkColumn annotation.
 *
 * @author rambabu.posa
 */
case class PhotoMetadataScala(
  dateTaken: Timestamp, //@SparkColumn(name = "Date")
  directory: String,
  extension: String,
  fileCreationDate: Timestamp,
  fileLastAccessDate: Timestamp,
  fileLastModifiedDate: Timestamp,
  filename: String,    //@SparkColumn(nullable = false)
  geoX: Float = 0.0f,  //@SparkColumn(`type` = "float")
  geoY: Float = 0.0f,
  geoZ: Float = 0.0f,
  height: Int,
  mimeType: String ,
  name: String,
  size: Long = 0L,
  width: Int = 0
)

// Pending methods
/**
  def setDateTaken(date: Date): Unit = {
    if (date == null) {
      PhotoMetadataScala.log.warn("Attempt to set a null date.")
      return
    }
    setDateTaken(new Timestamp(date.getTime))
  }

  def setFileCreationDate(creationTime: FileTime): Unit = {
    setFileCreationDate(new Timestamp(creationTime.toMillis))
  }

  def setFileLastAccessDate(lastAccessTime: FileTime): Unit = {
    setFileLastAccessDate(new Timestamp(lastAccessTime.toMillis))
  }

  def setFileLastModifiedDate(lastModifiedTime: FileTime): Unit = {
    setFileLastModifiedDate(new Timestamp(lastModifiedTime.toMillis))
  }
*/


