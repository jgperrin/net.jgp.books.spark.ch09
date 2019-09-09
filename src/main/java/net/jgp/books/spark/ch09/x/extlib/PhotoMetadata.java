package net.jgp.books.spark.ch09.x.extlib;

import java.io.Serializable;
import java.nio.file.attribute.FileTime;
import java.sql.Timestamp;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch09.x.utils.SparkColumn;

/**
 * A good old JavaBean containing the EXIF properties as well as the
 * SparkColumn annotation.
 * 
 * @author jgp
 */
public class PhotoMetadata implements Serializable {
  private static transient Logger log = LoggerFactory.getLogger(
      PhotoMetadata.class);
  private static final long serialVersionUID = -2589804417011601051L;

  private Timestamp dateTaken;
  private String directory;
  private String extension;
  private Timestamp fileCreationDate;
  private Timestamp fileLastAccessDate;
  private Timestamp fileLastModifiedDate;
  private String filename;
  private Float geoX;
  private Float geoY;
  private Float geoZ;
  private int height;
  private String mimeType;
  private String name;
  private long size;
  private int width;

  /**
   * @return the dateTaken
   */
  @SparkColumn(name = "Date")
  public Timestamp getDateTaken() {
    return dateTaken;
  }

  /**
   * @return the directory
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * @return the extension
   */
  public String getExtension() {
    return extension;
  }

  /**
   * @return the fileCreationDate
   */
  public Timestamp getFileCreationDate() {
    return fileCreationDate;
  }

  /**
   * @return the fileLastAccessDate
   */
  public Timestamp getFileLastAccessDate() {
    return fileLastAccessDate;
  }

  /**
   * @return the fileLastModifiedDate
   */
  public Timestamp getFileLastModifiedDate() {
    return fileLastModifiedDate;
  }

  /**
   * @return the filename
   */
  @SparkColumn(nullable = false)
  public String getFilename() {
    return filename;
  }

  /**
   * @return the geoX
   */
  @SparkColumn(type = "float")
  public Float getGeoX() {
    return geoX;
  }

  /**
   * @return the geoY
   */
  public Float getGeoY() {
    return geoY;
  }

  /**
   * @return the geoZ
   */
  public Float getGeoZ() {
    return geoZ;
  }

  /**
   * @return the height
   */
  public int getHeight() {
    return height;
  }

  /**
   * @return the mimeType
   */
  public String getMimeType() {
    return mimeType;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the size
   */
  public long getSize() {
    return size;
  }

  /**
   * @return the width
   */
  public int getWidth() {
    return width;
  }

  public void setDateTaken(Date date) {
    if (date == null) {
      log.warn("Attempt to set a null date.");
      return;
    }
    setDateTaken(new Timestamp(date.getTime()));
  }

  /**
   * @param dateTaken
   *          the dateTaken to set
   */
  public void setDateTaken(Timestamp dateTaken) {
    this.dateTaken = dateTaken;
  }

  /**
   * @param directory
   *          the directory to set
   */
  public void setDirectory(String directory) {
    this.directory = directory;
  }

  /**
   * @param extension
   *          the extension to set
   */
  public void setExtension(String extension) {
    this.extension = extension;
  }

  public void setFileCreationDate(FileTime creationTime) {
    setFileCreationDate(new Timestamp(creationTime.toMillis()));
  }

  /**
   * @param fileCreationDate
   *          the fileCreationDate to set
   */
  public void setFileCreationDate(Timestamp fileCreationDate) {
    this.fileCreationDate = fileCreationDate;
  }

  public void setFileLastAccessDate(FileTime lastAccessTime) {
    setFileLastAccessDate(new Timestamp(lastAccessTime.toMillis()));
  }

  /**
   * @param fileLastAccessDate
   *          the fileLastAccessDate to set
   */
  public void setFileLastAccessDate(Timestamp fileLastAccessDate) {
    this.fileLastAccessDate = fileLastAccessDate;
  }

  public void setFileLastModifiedDate(FileTime lastModifiedTime) {
    setFileLastModifiedDate(new Timestamp(lastModifiedTime.toMillis()));
  }

  /**
   * @param fileLastModifiedDate
   *          the fileLastModifiedDate to set
   */
  public void setFileLastModifiedDate(Timestamp fileLastModifiedDate) {
    this.fileLastModifiedDate = fileLastModifiedDate;
  }

  /**
   * @param filename
   *          the filename to set
   */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * @param geoX
   *          the geoX to set
   */
  public void setGeoX(Float geoX) {
    this.geoX = geoX;
  }

  /**
   * @param geoY
   *          the geoY to set
   */
  public void setGeoY(Float geoY) {
    this.geoY = geoY;
  }

  /**
   * @param geoZ
   *          the geoZ to set
   */
  public void setGeoZ(Float geoZ) {
    this.geoZ = geoZ;
  }

  /**
   * @param height
   *          the height to set
   */
  public void setHeight(int height) {
    this.height = height;
  }

  /**
   * @param mimeType
   *          the mimeType to set
   */
  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }

  /**
   * @param name
   *          the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @param size
   *          the size to set
   */
  public void setSize(long size) {
    this.size = size;
  }

  /**
   * @param width
   *          the width to set
   */
  public void setWidth(int width) {
    this.width = width;
  }

}
