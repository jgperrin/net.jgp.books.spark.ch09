package net.jgp.books.sparkWithJava.ch09.lab_100.photo_datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PhotoMetadataIngestionApp {
    public static void main(String[] args) {
        PhotoMetadataIngestionApp app = new PhotoMetadataIngestionApp();
        app.start();
    }

    private boolean start() {
        SparkSession spark = SparkSession.builder()
                .appName("EXIF to Dataset")
                .master("local[*]").getOrCreate();
        
        String importDirectory = "/Users/jgp/Pictures";
        
        Dataset<Row> df = spark.read()
                .format("exif")
                .option("recursive", "true")
                .option("limit", "100000")
                .option("extensions", "jpg,jpeg")
                .load(importDirectory);
        
        // We can start analytics
        df = df
                .filter(df.col("GeoX").isNotNull())
                .filter(df.col("GeoZ").notEqual("NaN"))
                .orderBy(df.col("GeoZ").desc());
        df.collect();
        df.cache();
        System.out.println("I have imported " + df.count() + " photos.");
        df.printSchema();
        df.show(5);
        
        return true;
    }
}
