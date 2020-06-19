package net.jgp.books.spark.ch09.lab101_photo_datasource_no_short_name

import org.apache.spark.sql.SparkSession

/**
 * Ingest metadata from a directory containing photos, make them available
 * as EXIF.
 *
 * @author rambabu.posa
 */
object PhotoMetadataIngestionNoShortNameScalaApp {

  /**
   * Starts the application
   *
   */
  def main(args: Array[String]): Unit = {

    // Get a session
    val spark = SparkSession.builder
      .appName("EXIF to Dataset")
      .master("local")
      .getOrCreate

    // Import directory
    val importDirectory = "data"

    // read the data
    val df = spark.read
      .format("net.jgp.books.spark.ch09.x.ds.exif.ExifDirectoryDataSourceShortnameAdvertiser")
      .option("recursive", "true")
      .option("limit", "100000")
      .option("extensions", "jpg,jpeg")
      .load(importDirectory)

    println(s"I have imported ${df.count} photos.")

    df.printSchema()
    df.show(5)

  }

}
