package net.jgp.books.spark.ch09.lab400_photo_datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Ingest metadata from a directory containing photos, make them available
 * as EXIF.
 * 
 * @author jgp
 */
public class PhotoMetadataIngestionApp {
  public static void main(String[] args) {
    PhotoMetadataIngestionApp app = new PhotoMetadataIngestionApp();
    app.start();
  }

  /**
   * Starts the application
   * 
   * @return <code>true</code> if all is ok.
   */
  private boolean start() {
    // Get a session
    SparkSession spark = SparkSession.builder()
        .appName("EXIF to Dataset")
        .master("local").getOrCreate();

    // Import directory
    String importDirectory = "data";

    // read the data
    Dataset<Row> df = spark.read()
        .format("exif")
        .option("recursive", "true")
        .option("limit", "100000")
        .option("extensions", "jpg,jpeg")
        .load(importDirectory);

    System.out.println("I have imported " + df.count() + " photos.");
    df.printSchema();
    df.show(5);

    return true;
  }
}
