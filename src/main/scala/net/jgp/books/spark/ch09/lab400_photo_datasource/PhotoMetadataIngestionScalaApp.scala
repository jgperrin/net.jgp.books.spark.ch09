package net.jgp.books.spark.ch09.lab400_photo_datasource

import org.apache.spark.sql.SparkSession

/**
 * Ingest metadata from a directory containing photos, make them available
 * as EXIF.
 *
 * @author rambabu.posa
 */
object PhotoMetadataIngestionScalaApplication {

  /**
   * Starts the application
   *
   * @return <code>true</code> if all is ok.
   */
  def main(args: Array[String]): Unit = {
    val app = new PhotoMetadataIngestionScalaApp
    app.start
  }
}

class PhotoMetadataIngestionScalaApp {
  /**
   * Starts the application
   *
   * @return <code>true</code> if all is ok.
   */
  def start(): Boolean = {
    // Get a session
    val spark = SparkSession.builder
      .appName("EXIF to Dataset")
      .master("local[*]")
      .getOrCreate

    // Import directory
    val importDirectory = "data"

    // read the data
    val df = spark.read
      .format("exif")
      .option("recursive", "true")
      .option("limit", "100000")
      .option("extensions", "jpg,jpeg")
      .load(importDirectory)

    println(s"I have imported ${df.count} photos.")
    df.printSchema()
    df.show(5)

    true
  }

}
