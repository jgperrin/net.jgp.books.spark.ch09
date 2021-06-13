The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean-Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition - chapter 9

Welcome to Spark in Action, 2e, chapter 9. This chapter covers advanced ingestion, including writing your own custom data source with Java.

This code is designed to work with Apache Spark v3.1.2.

## Lab

Each chapter has one or more labs. Labs are examples used for teaching in the [book](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**.

### Lab \#400

The `PhotoMetadataIngestionApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It asks Spark to load (ingest) a dataset in 'exif' format.
3.	Spark stores the contents in a dataframe, then print the records.

### Lab \#401

Similar to lab #400, without using the short name.

## Running the lab in Java

For information on running the Java lab, see chapter 1 in [Spark in Action, 2nd edition](http://jgp.net/sia).


## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - "Spark in production: installation and a few tips"). 

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch09

2. cd net.jgp.books.spark.ch09

3. Package application using sbt command

   ```
     sbt clean assembly
   ```

4. Run Spark/Scala application using spark-submit command as shown below:

   ```
   spark-submit --class net.jgp.books.spark.ch09.lab400_photo_datasource.PhotoMetadataIngestionScalaApp target/scala-2.12/SparkInAction2-Chapter09-assembly-1.0.0.jar  
   ```

## News

 1. [2020-06-13] Updated the `pom.xml` to support Apache Spark v3.1.2. 
 1. [2020-06-13] As we celebrate the first anniversary of Spark in Action, 2nd edition is the best-rated Apache Spark book on [Amazon](https://amzn.to/2TPnmOv). 
 
## Notes

 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 1. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 1. The master branch contains the last version of the code running against the latest supported version of Apache Spark. Look in specifics branches for specific versions.
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://fb.com/SparkInAction/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
