package net.jgp.books.spark.ch09.x.extlib

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.util.TimeZone
import com.drew.imaging.ImageMetadataReader
import com.drew.imaging.ImageProcessingException
import com.drew.lang.Rational
import com.drew.metadata.{Metadata, MetadataException}
import com.drew.metadata.exif.ExifSubIFDDirectory
import com.drew.metadata.exif.GpsDirectory
import com.drew.metadata.jpeg.JpegDirectory

object ExifUtilsScala {

  def processFromFilename(absolutePathToPhoto: String): PhotoMetadata = {
    val photo = new PhotoMetadata
    photo.setFilename(absolutePathToPhoto)
    // Info from file
    val photoFile = new File(absolutePathToPhoto)
    photo.setName(photoFile.getName)
    photo.setSize(photoFile.length)
    photo.setDirectory(photoFile.getParent)
    val file = Paths.get(absolutePathToPhoto)
    var attr:BasicFileAttributes = null
    try attr = Files.readAttributes(file, classOf[BasicFileAttributes])
    catch {
      case e: IOException =>
        println(f"I/O error while reading attributes of $absolutePathToPhoto. Got ${e.getMessage}. Ignoring attributes.")
    }

    if (attr != null) {
      photo.setFileCreationDate(attr.creationTime)
      photo.setFileLastAccessDate(attr.lastAccessTime)
      photo.setFileLastModifiedDate(attr.lastModifiedTime)
    }
    // Extra info
    photo.setMimeType("image/jpeg")
    val extension = absolutePathToPhoto.substring(absolutePathToPhoto.lastIndexOf('.') + 1)
    photo.setExtension(extension)
    // Info from EXIF
    var metadata: Metadata = null
    try metadata = ImageMetadataReader.readMetadata(photoFile)
    catch {
      case e: ImageProcessingException =>
        println(s"Image processing exception while reading $absolutePathToPhoto. Got ${e.getMessage}. Cannot extract EXIF Metadata.")
      case e: IOException =>
        println(s"I/O error while reading $absolutePathToPhoto. Got ${e.getMessage}. Cannot extract EXIF Metadata.")
    }
    if (metadata == null) return photo
    val jpegDirectory = metadata.getFirstDirectoryOfType(classOf[JpegDirectory])
    try {
      photo.setHeight(jpegDirectory.getInt(1))
      photo.setWidth(jpegDirectory.getInt(3))
    } catch {
      case e: MetadataException =>
        println(s"Issue while extracting dimensions from $absolutePathToPhoto. Got ${e.getMessage}. Ignoring dimensions.")
    }
    val exifSubIFDDirectory = metadata.getFirstDirectoryOfType(classOf[ExifSubIFDDirectory])
    if (exifSubIFDDirectory != null) {
      val d = exifSubIFDDirectory.getDate(36867, TimeZone.getTimeZone("EST"))
      if (d != null) photo.setDateTaken(d)
    }
    val gpsDirectory = metadata.getFirstDirectoryOfType(classOf[GpsDirectory])
    if (gpsDirectory != null) {
      photo.setGeoX(getDecimalCoordinatesAsFloat(gpsDirectory.getString(1), gpsDirectory.getRationalArray(2)))

      try photo.setGeoY(getDecimalCoordinatesAsFloat(gpsDirectory.getString(3), gpsDirectory.getRationalArray(4)))
      catch {
        case e: Exception =>
          println(s"Issue while extracting longitude GPS info from $absolutePathToPhoto. Got ${e.getMessage} (${e.getClass.getName}). Ignoring GPS info.")
      }
      try {
        val r = gpsDirectory.getRational(6)
        if (r != null) photo.setGeoZ(1f * r.getNumerator / r.getDenominator)
      } catch {
        case e: Exception =>
          println(s"Issue while extracting altitude GPS info from $absolutePathToPhoto. Got ${e.getMessage} (${e.getClass.getName}). Ignoring GPS info.")
      }
    }
    photo
  }

  private def getDecimalCoordinatesAsFloat(orientation: String, coordinates: Array[Rational]): Float = {
    if (orientation == null) {
      println("GPS orientation is null, should be N, S, E, or W.")
      return Float.MaxValue
    }
    if (coordinates == null) {
      println("GPS coordinates are null.")
      return Float.MaxValue
    }
    if (coordinates(0).getDenominator == 0 || coordinates(1).getDenominator == 0 || coordinates(2).getDenominator == 0) {
      println("Invalid GPS coordinates (denominator should not be 0).")
      return Float.MaxValue
    }
    var m = 1
    if (orientation.toUpperCase.charAt(0) == 'S' || orientation.toUpperCase.charAt(0) == 'W') m = -1
    val deg = coordinates(0).getNumerator / coordinates(0).getDenominator
    val min = coordinates(1).getNumerator * 60 * coordinates(2).getDenominator
    val sec = coordinates(2).getNumerator
    val den = 3600 * coordinates(1).getDenominator * coordinates(2).getDenominator
    m * (deg + (min + sec) / den)
  }

}
