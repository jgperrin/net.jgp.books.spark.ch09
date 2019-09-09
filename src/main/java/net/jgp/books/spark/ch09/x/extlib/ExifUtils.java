package net.jgp.books.spark.ch09.x.extlib;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.lang.Rational;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.MetadataException;
import com.drew.metadata.exif.ExifSubIFDDirectory;
import com.drew.metadata.exif.GpsDirectory;
import com.drew.metadata.jpeg.JpegDirectory;

public class ExifUtils {
  private static transient Logger log = LoggerFactory.getLogger(
      ExifUtils.class);

  public static PhotoMetadata
      processFromFilename(String absolutePathToPhoto) {
    PhotoMetadata photo = new PhotoMetadata();
    photo.setFilename(absolutePathToPhoto);

    // Info from file
    File photoFile = new File(absolutePathToPhoto);
    photo.setName(photoFile.getName());
    photo.setSize(photoFile.length());
    photo.setDirectory(photoFile.getParent());

    Path file = Paths.get(absolutePathToPhoto);
    BasicFileAttributes attr = null;
    try {
      attr = Files.readAttributes(file, BasicFileAttributes.class);
    } catch (IOException e) {
      log.warn(
          "I/O error while reading attributes of {}. Got {}. Ignoring attributes.",
          absolutePathToPhoto,
          e.getMessage());
    }
    if (attr != null) {
      photo.setFileCreationDate(attr.creationTime());
      photo.setFileLastAccessDate(attr.lastAccessTime());
      photo.setFileLastModifiedDate(attr.lastModifiedTime());
    }

    // Extra info
    photo.setMimeType("image/jpeg");
    String extension = absolutePathToPhoto.substring(absolutePathToPhoto
        .lastIndexOf('.') + 1);
    photo.setExtension(extension);

    // Info from EXIF
    Metadata metadata = null;
    try {
      metadata = ImageMetadataReader.readMetadata(photoFile);
    } catch (ImageProcessingException e) {
      log.warn(
          "Image processing exception while reading {}. Got {}. Cannot extract EXIF Metadata.",
          absolutePathToPhoto,
          e.getMessage());
    } catch (IOException e) {
      log.warn(
          "I/O error while reading {}. Got {}. Cannot extract EXIF Metadata.",
          absolutePathToPhoto, e.getMessage());
    }
    if (metadata == null) {
      return photo;
    }

    Directory jpegDirectory = metadata.getFirstDirectoryOfType(
        JpegDirectory.class);
    try {
      photo.setHeight(jpegDirectory.getInt(1));
      photo.setWidth(jpegDirectory.getInt(3));
    } catch (MetadataException e) {
      log.warn(
          "Issue while extracting dimensions from {}. Got {}. Ignoring dimensions.",
          absolutePathToPhoto,
          e.getMessage());
    }

    Directory exifSubIFDDirectory = metadata.getFirstDirectoryOfType(
        ExifSubIFDDirectory.class);
    if (exifSubIFDDirectory != null) {
      Date d =
          exifSubIFDDirectory.getDate(36867, TimeZone.getTimeZone("EST"));
      if (d != null) {
        photo.setDateTaken(d);
      }
    }

    Directory gpsDirectory = metadata.getFirstDirectoryOfType(
        GpsDirectory.class);
    if (gpsDirectory != null) {
      // try {
      photo.setGeoX(getDecimalCoordinatesAsFloat(gpsDirectory.getString(1),
          gpsDirectory.getRationalArray(2)));
      // } catch (Exception e) {
      // log.warn("Issue while extracting latitude GPS info from {}. Got {}
      // ({}).
      // Ignoring GPS info.", absolutePathToPhoto,
      // e.getMessage(), e.getClass().getName());
      // }
      try {
        photo
            .setGeoY(getDecimalCoordinatesAsFloat(gpsDirectory.getString(3),
                gpsDirectory.getRationalArray(4)));
      } catch (Exception e) {
        log.warn(
            "Issue while extracting longitude GPS info from {}. Got {} ({}). Ignoring GPS info.",
            absolutePathToPhoto,
            e.getMessage(), e.getClass().getName());
      }
      try {
        Rational r = gpsDirectory.getRational(6);
        if (r != null) {
          photo.setGeoZ(1f * r.getNumerator() / r.getDenominator());
        }
      } catch (Exception e) {
        log.warn(
            "Issue while extracting altitude GPS info from {}. Got {} ({}). Ignoring GPS info.",
            absolutePathToPhoto,
            e.getMessage(), e.getClass().getName());
      }
    }
    return photo;
  }

  private static Float getDecimalCoordinatesAsFloat(String orientation,
      Rational[] coordinates) {
    if (orientation == null) {
      log.debug("GPS orientation is null, should be N, S, E, or W.");
      return null;
    }
    if (coordinates == null) {
      log.debug("GPS coordinates are null.");
      return null;
    }
    if (coordinates[0].getDenominator() == 0 || coordinates[1]
        .getDenominator() == 0
        || coordinates[2].getDenominator() == 0) {
      log.debug("Invalid GPS coordinates (denominator should not be 0).");
      return null;
    }

    float m = 1;
    if (orientation.toUpperCase().charAt(0) == 'S'
        || orientation.toUpperCase()
            .charAt(0) == 'W') {
      m = -1;
    }
    float deg =
        coordinates[0].getNumerator() / coordinates[0].getDenominator();
    float min = coordinates[1].getNumerator() * 60 * coordinates[2]
        .getDenominator();
    float sec = coordinates[2].getNumerator();
    float den = 3600 * coordinates[1].getDenominator() * coordinates[2]
        .getDenominator();
    return m * (deg + (min + sec) / den);
  }
}
